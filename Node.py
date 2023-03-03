import argparse
import grpc
import json
import logging
import random
import threading

from concurrent import futures
from typing import Tuple

import raft_pb2
import raft_pb2_grpc

from logging.handlers import RotatingFileHandler


class Entry:
    def __init__(self, term, op, data):
        self.term = term
        self.op = op
        self.data = data

    def __repr__(self):
        return "{}:{}:{}".format(self.term, self.op, self.data)


class NodeStatus:
    def __init__(self, id, ip, port, health='healthy'):
        self.id = id
        self.ip = ip
        self.port = port
        self.health = health
        self._timeout_cycle = 0

    def __repr__(self):
        return "Node%s - %s:%s" % (self.id, self.ip, self.port)

    def is_healthy(self):
        return self.health == "healthy"

    def timeout(self):
        self._timeout_cycle += 1
        logging.info("Node%s - timeout cycle: %s" %
                     (self.id, self._timeout_cycle))
        if self._timeout_cycle >= 10:
            self.health = "unhealthy"

    def reset_timeout(self):
        self._timeout_cycle = 0
        self.health = "healthy"


class RaftServicer(raft_pb2_grpc.RaftNodeServicer):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
    HB_TIME = 1
    ELECTION_TIME_LOW = 4 * HB_TIME
    ELECTION_TIME_HIGH = 8 * HB_TIME
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"

    def __init__(self, node_id: str, peers: list[NodeStatus] = None,
                 cur_term: int = 0, log_entries: list[Entry] = None) -> None:
        self._lock = threading.Lock()
        self._role = self.FOLLOWER
        self._cur_term = cur_term
        self._id = node_id
        self._vote_for = None
        if log_entries is None:
            self._log_entries = []
        else:
            self._log_entries = log_entries
        self._commit_index = 0
        self._last_applied = -1
        self._peers = peers
        self._granted = 0
        self._election_timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._election_timer.start()
        self._hb_timer = None
        # it will not be empty if this node is leader. key is node id and value
        # is prev_index
        self._nodes_prev_index = {}
        # leader id
        self._leader_id = None
        self._data_file = "Node{}.json".format(node_id)
        self._data_content = {}
        try:
            with open(self._data_file) as f:
                self._data_content = json.load(f)
        except FileNotFoundError:
            self._data_content['term'] = self._cur_term
            with open(self._data_file, 'w+') as f:
                json.dump(self._data_file, f)

    def heartbeat_nodes(self):
        prev_log_index = len(self._log_entries) - 1
        prev_log_term = \
            0 if prev_log_index else self._log_entries[prev_log_index].term
        rpc = raft_pb2.MsgAppendEntriesRequest(term=self._cur_term, leaderId=self._id, prevLogIndex=prev_log_index,
                                               prevLogTerm=prev_log_term, leaderCommit=self._commit_index,
                                               entries=[])
        self.send_append_entries_to_all(rpc)
        logging.info("Node %s heart beating follower nodes: %s",
                     self._id, rpc)
        # Start heart beat timer
        self._hb_timer = threading.Timer(self.HB_TIME,
                                         self.heartbeat_nodes)
        self._hb_timer.start()

    def persist_term_and_vote(self):
        self._data_content['term'] = self._cur_term
        self._data_content['vote_for'] = self._vote_for
        with open(self._data_file, 'w+') as out:
            json.dump(self._data_content, out)

    def persist_entries(self):
        self._data_content['entries'] = self._log_entries
        with open(self._data_file, 'w+') as out:
            json.dump(self._data_content, out)

    def leader_elect_timeout_handler(self):
        with self._lock:
            if self._role == self.LEADER:
                logging.debug("I am leader, exit")
                return

        logging.info("Start a new vote cycle for term: %s",
                     self._cur_term)
        with self._lock:
            self._role = self.CANDIDATE
            self._vote_for = None
            self._granted = 0
            self._cur_term += 1
            self.persist_term_and_vote()
        self.send_request_vote()
        self.restart_election_timer()

    def restart_election_timer(self):
        logging.info("Restart election timer")
        self._election_timer.cancel()
        self._election_timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._election_timer.start()

    @staticmethod
    def send_append_entries_message(id: str, ip: str, port: int, msg: raft_pb2.MsgAppendEntriesRequest) \
            -> Tuple[str, raft_pb2.MsgAppendEntriesResponse]:
        logging.debug('Send append entries %s to %s:%s', msg, ip, port)
        while True:
            try:
                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    return id, stub.AppendEntries(msg)
            except:
                logging.exception(f"Failed to send append entries message to {ip}:{port}, retry")

    def handle_append_entries_response(self, id: str, rsp: raft_pb2.MsgAppendEntriesResponse) -> None:
        while not rsp.success:
            prev_log_index = \
                self._nodes_prev_index.getdefault(
                    id, len(self._log_entries) - 1) - 1
            if prev_log_index < -1:
                prev_log_index = -1
            prev_log_term = 0 if not self._log_entries else \
                self._log_entries[prev_log_index].term
            self._nodes_prev_index[id] = prev_log_index
            rpc_msg = raft_pb2.MsgAppendEntriesRequest(term=self._cur_term,
                                                       leaderId=self._id,
                                                       prevLogIndex=prev_log_index,
                                                       prevLogTerm=prev_log_term,
                                                       entries=self._log_entries[prev_log_index:],
                                                       leaderCommit=self._commit_index)
            for peer in self._peers:
                if peer.id == id:
                    id_, resp_ = self.send_append_entries_message(id, peer.ip, peer.port, rpc_msg)
                    break

    def send_append_entries_to_all(self, msg: raft_pb2.MsgAppendEntriesRequest) -> None:
        f_list = []
        with futures.ThreadPoolExecutor(max_workers=4) as t:
            for peer in self._peers:
                logging.debug("Send AppendEntries to Node%s - %s:%s", peer.id, peer.ip, peer.port)
                f = t.submit(self.send_append_entries_message, peer.id, peer.ip, peer.port, msg)
                f_list.append(f)
            for f in futures.as_completed(f_list):
                id, resp = f.result()
                self.handle_append_entries_response(id, resp)

    def handle_vote_response(self, resp: raft_pb2.MsgVoteResponse) -> None:
        if resp.voteGranted:
            switch_to_leader = False
            with self._lock:
                self._granted += 1
                logging.debug("%s Granted me as leader", self._granted)
                # Win election, change role to leader
                if self._granted > len(self._peers) / 2:
                    switch_to_leader = True
            if switch_to_leader:
                with self._lock:
                    self._role = self.LEADER
                    self._leader_id = self._id
                logging.info("I am the leader")
                print("I am leader")
                # Start to send AppendEntriesRequest to peer nodes
                last_log_index = len(self._log_entries) - 1
                last_log_term = \
                    0 if last_log_index == -1 \
                        else self._log_entries[last_log_index].term
                rpc_msg = raft_pb2.MsgAppendEntriesRequest(term=self._cur_term, leaderId=self._id,
                                                           prevLogIndex=last_log_index, prevLogTerm=last_log_term,
                                                           entries=self._log_entries, leaderCommit=self._commit_index)
                # Start heart beat timer
                self._hb_timer = threading.Timer(self.HB_TIME,
                                                 self.heartbeat_nodes)
                self._hb_timer.start()

                # stop election timer
                if self._election_timer:
                    self._election_timer.cancel()
                self.send_append_entries_to_all(rpc_msg)
        else:
            # update term with response term
            if resp.term > self._cur_term:
                with self._lock:
                    self._cur_term = resp.term

    @staticmethod
    def send_request_vote_message(ip: str, port: int, msg: raft_pb2.MsgVoteRequest) \
            -> raft_pb2.MsgVoteResponse:
        logging.info('Send request vote %s to %s:%s', msg, ip, port)
        while True:
            try:
                with grpc.insecure_channel(f'{ip}:{port}') as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    return stub.RequestVote(msg)
            except:
                logging.exception(f"Failed to send request vote message to {ip}:{port}, retry")

    def send_request_vote_to_all(self, msg: raft_pb2.MsgVoteRequest) -> None:
        # Grant myself at first
        with self._lock:
            if self._vote_for is None:
                self._granted += 1
                self._vote_for = self._id
        f_list = []
        with futures.ThreadPoolExecutor(max_workers=4) as t:
            for peer in self._peers:
                f = t.submit(self.send_request_vote_message, peer.ip, peer.port, msg)
                f_list.append(f)
            for f in futures.as_completed(f_list):
                resp = f.result()
                self.handle_vote_response(resp)

    def send_request_vote(self):
        last_log_index = len(self._log_entries) - 1
        if last_log_index > -1:
            last_log_term = self._log_entries[last_log_index].term
        else:
            last_log_term = 0
        rpc_message = raft_pb2.MsgVoteRequest(term=self._cur_term, candidateId=self._id,
                                              lastLogIndex=last_log_index,
                                              lastLogTerm=last_log_term)
        if not self._peers:
            self._role = self.LEADER
        else:
            self.send_request_vote_to_all(rpc_message)

    def RequestVote(self, request, context):
        term_in_request = request.term
        resp = raft_pb2.MsgVoteResponse(term=self._cur_term, voteGranted=False)
        term_in_log = self._log_entries[-1].term if self._log_entries else -1
        last_log_index = len(self._log_entries) - 1
        logging.info("Process vote request: %s", request)
        if term_in_request >= self._cur_term:
            if self._vote_for is None and \
                    (request.lastLogTerm > term_in_log or
                     (request.lastLogTerm == term_in_log and
                      request.lastLogIndex >= last_log_index)):
                resp.voteGranted = True
                resp.term = term_in_request
                self._vote_for = request.candidateId
                self._cur_term = term_in_request
                print("Vote for: %s, term_in_msg: %s, term_in_log: %s"
                      ", index_in_msg: %s, last_log_index: %s" %
                      (self._vote_for, term_in_request, term_in_log,
                       request.lastLogIndex, last_log_index))
        return resp

    def AppendEntries(self, request: raft_pb2.MsgAppendEntriesRequest, context):
        logging.debug("Process append entries request: %s", request)
        # Received AppendEntries means this node is follower
        if self._hb_timer is not None:
            self._hb_timer.cancel()
            self._hb_timer = None
        self.restart_election_timer()
        # AppendEntriesRequest message fields:
        # APPEND_ENTRY_REQUEST:leader_id:term:prev_log_index:
        # prev_log_term:entries count:entry[0]...:commit_log_index
        term_in_msg = request.term
        leader_id = int(request.leaderId)
        if term_in_msg < self._cur_term:
            logging.warning("Term in AppendEntriesRequest: %s  from %s"
                            "< my own term: %s", term_in_msg, leader_id,
                            self._cur_term)
            return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=False)

        with self._lock:
            self._role = self.FOLLOWER
            self._leader_id = leader_id
            self._cur_term = term_in_msg
            self.persist_term_and_vote()

        my_last_log_index = len(self._log_entries) - 1
        my_last_log_term = \
            0 if my_last_log_index == -1 else self._log_entries[
                my_last_log_index]
        prev_log_index_in_msg = request.prevLogIndex
        prev_log_term_in_msg = request.prevLogTerm
        if my_last_log_index < prev_log_index_in_msg:
            return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=False)
        if my_last_log_term != prev_log_term_in_msg:
            self._log_entries = self._log_entries[:prev_log_index_in_msg]
        if request.entries:
            self._log_entries.extend(request.entries)
            self.persist_entries()
        with self._lock:
            if request.leaderCommit > self._commit_index:
                self._commit_index = min(request.leaderCommit, len(self._log_entries) - 1)
        return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=True)


def serve(port, node_id, neighbors, cur_term, entries):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftNodeServicer_to_server(
        RaftServicer(node_id, neighbors, cur_term, entries), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server started, listening on {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--conf", help="config file path", default='config.json')
    # Node id is an integer. It requires there is corresponding
    # section 'Node'+ id exists in config file specified by
    # --conf option. For example, if node id is 0, there should
    # be a section 'Node0' in config file
    arg_parser.add_argument("--node", help="id of node")
    args = arg_parser.parse_args()
    # config = {}
    with open(args.conf) as f:
        config = json.load(f)
    if args.node not in config:
        logging.error(f"{args.node} not found in {args.conf}")
        exit(-1)
    node_ip = config[args.node]['ip']
    node_port = config[args.node]['port']
    peers = [NodeStatus(n, config[n]['ip'], config[n]['port']) \
             for n in config['members'] if n != args.node]
    term = 0
    entries = []
    with open('Node{}.json'.format(args.node), 'w+') as f:
        try:
            data = json.load(f)
            term = data['term']
            entries = data['entries']
        except:
            data = {}
            data['term'] = term
            data['entries'] = entries
            json.dump(data, f)

    logging.basicConfig(
        handlers=[RotatingFileHandler(
            f'Node{args.node}.log', maxBytes=2 * 1024 * 1024, backupCount=10)],
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)s [%(funcName)s] %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S')

    logging.info("Node run on %s:%s with id %s", node_ip, node_port, args.node)
    logging.info("Peer nodes: %s", peers)
    serve(node_port, args.node, peers, term, entries)
