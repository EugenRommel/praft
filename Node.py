import argparse
import os
import time

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

LOG_DIR = "log"
DATA_DIR = "data"


class Entry:
    def __init__(self, term, op, data):
        self.term = term
        self.op = op
        self.data = data

    def __repr__(self):
        return "{}:{}:{}".format(self.term, self.op, self.data)

    def to_dict(self):
        return {"term": self.term, "op": self.op, "data": self.data}

    @staticmethod
    def from_dict(d):
        return Entry(d["term"], d["op"], d["data"])


def entry_to_proto(entry: Entry) -> raft_pb2.Entry:
    return raft_pb2.Entry(term=entry.term, op=entry.op, data=str(entry.data))


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
    RPC_TIMEOUT = 1.0
    COMMIT_TIMEOUT = 5.0
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"

    def __init__(self, node_id: str, peers: list[NodeStatus] = None,
                 cur_term: int = 0, log_entries: list[Entry] = None) -> None:
        self._lock = threading.RLock()
        self._role = self.FOLLOWER
        self._cur_term = cur_term
        self._id = node_id
        self._vote_for = None
        self._log_entries = list(log_entries) if log_entries is not None else []
        self._commit_index = -1
        self._last_applied = -1
        self._state = {}
        self._peers = list(peers) if peers is not None else []
        self._granted = 0
        self._election_timer = None
        self._hb_timer = None
        self._leader_id = None
        # Leader only. next index of next log entry to send to a follower,
        # match index of the highest entry known to be replicated on follower.
        self._next_index = {}
        self._match_index = {}
        self._last_append_request = {}
        self._rpc_executor = futures.ThreadPoolExecutor(max_workers=8)
        self._data_file = os.path.join(DATA_DIR, "Node{}.json".format(node_id))
        self._data_content = {}
        try:
            with open(self._data_file) as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                self._data_content = loaded
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        if "term" in self._data_content:
            self._cur_term = self._data_content["term"]
        if "vote_for" in self._data_content:
            self._vote_for = self._data_content["vote_for"]
        if "entries" in self._data_content:
            self._log_entries = [Entry.from_dict(e)
                                 for e in self._data_content["entries"]]
        self.persist_term_and_vote()
        self.restart_election_timer()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def persist_term_and_vote(self):
        self._data_content['term'] = self._cur_term
        self._data_content['vote_for'] = self._vote_for
        with open(self._data_file, 'w') as out:
            json.dump(self._data_content, out)

    def persist_entries(self):
        self._data_content['entries'] = [e.to_dict()
                                         for e in self._log_entries]
        with open(self._data_file, 'w') as out:
            json.dump(self._data_content, out)

    # ------------------------------------------------------------------
    # Role transitions
    # ------------------------------------------------------------------
    def _become_leader(self):
        with self._lock:
            if self._role == self.LEADER:
                return
            self._role = self.LEADER
            self._leader_id = self._id
            last_log_index = len(self._log_entries) - 1
            for p in self._peers:
                self._next_index[p.id] = last_log_index + 1
                self._match_index[p.id] = -1
        if self._election_timer is not None:
            self._election_timer.cancel()
        logging.info("I am the leader")
        print("I am leader")
        self._maybe_advance_commit()
        self._hb_timer = threading.Timer(self.HB_TIME, self.heartbeat_nodes)
        self._hb_timer.daemon = True
        self._hb_timer.start()
        self.send_append_entries_to_all()

    def _step_down(self, term: int):
        with self._lock:
            self._role = self.FOLLOWER
            self._cur_term = term
            self._vote_for = None
            self._granted = 0
            self.persist_term_and_vote()
        self._stop_heartbeat()
        self.restart_election_timer()

    def _stop_heartbeat(self):
        if self._hb_timer is not None:
            self._hb_timer.cancel()
            self._hb_timer = None

    # ------------------------------------------------------------------
    # State machine apply
    # ------------------------------------------------------------------
    def _apply_entry(self, entry: Entry):
        if entry.op == "set":
            key, _, value = entry.data.partition(":")
            self._state[key] = value

    def _apply_committed(self):
        with self._lock:
            while self._last_applied < self._commit_index:
                self._last_applied += 1
                self._apply_entry(self._log_entries[self._last_applied])

    def _maybe_advance_commit(self):
        with self._lock:
            if self._role != self.LEADER:
                return
            last_log_index = len(self._log_entries) - 1
            if last_log_index <= self._commit_index:
                return
            majority = (len(self._peers) + 1) // 2
            for n in range(last_log_index, self._commit_index, -1):
                if self._log_entries[n].term != self._cur_term:
                    continue
                matched = 1  # self
                for p in self._peers:
                    if self._match_index.get(p.id, -1) >= n:
                        matched += 1
                if matched > majority:
                    self._commit_index = n
                    break
            self._apply_committed()

    # ------------------------------------------------------------------
    # Leader: heartbeats and log replication
    # ------------------------------------------------------------------
    def _append_entries_request(self, id: str) -> raft_pb2.MsgAppendEntriesRequest:
        with self._lock:
            last_log_index = len(self._log_entries) - 1
            next_index = min(self._next_index.get(id, last_log_index + 1),
                             last_log_index + 1)
            prev_log_index = next_index - 1
            prev_log_term = 0 if prev_log_index < 0 else \
                self._log_entries[prev_log_index].term
            entries = [entry_to_proto(e)
                       for e in self._log_entries[next_index:]]
            self._last_append_request[id] = prev_log_index + len(entries)
            return raft_pb2.MsgAppendEntriesRequest(
                term=self._cur_term, leaderId=self._id,
                prevLogIndex=prev_log_index, prevLogTerm=prev_log_term,
                entries=entries, leaderCommit=self._commit_index)

    def heartbeat_nodes(self):
        with self._lock:
            if self._role != self.LEADER:
                return
        logging.debug("Node %s heart beating follower nodes", self._id)
        self.send_append_entries_to_all()
        self._hb_timer = threading.Timer(self.HB_TIME, self.heartbeat_nodes)
        self._hb_timer.daemon = True
        self._hb_timer.start()

    @staticmethod
    def send_append_entries_message(id: str, ip: str, port: int,
                                    msg: raft_pb2.MsgAppendEntriesRequest) \
            -> Tuple[str, raft_pb2.MsgAppendEntriesResponse]:
        try:
            with grpc.insecure_channel(f'{ip}:{port}') as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                return id, stub.AppendEntries(msg, timeout=RaftServicer.RPC_TIMEOUT)
        except Exception:
            logging.warning("Failed to send append entries to %s:%s", ip, port)
            return id, None

    def _send_append_and_handle(self, peer: NodeStatus,
                                msg: raft_pb2.MsgAppendEntriesRequest) -> None:
        id, rsp = self.send_append_entries_message(peer.id, peer.ip, peer.port, msg)
        self.handle_append_entries_response(id, rsp)

    def send_append_entries_to_all(self) -> None:
        with self._lock:
            peers = list(self._peers)
        f_list = []
        for p in peers:
            msg = self._append_entries_request(p.id)
            f_list.append(self._rpc_executor.submit(
                self._send_append_and_handle, p, msg))
        for f in futures.as_completed(f_list):
            try:
                f.result()
            except Exception:
                continue

    def _send_append_entries_to(self, id: str) -> None:
        peer = next((p for p in self._peers if p.id == id), None)
        if peer is None:
            return
        msg = self._append_entries_request(id)
        self._rpc_executor.submit(self._send_append_and_handle, peer, msg)

    def handle_append_entries_response(self, id: str,
                                       rsp: raft_pb2.MsgAppendEntriesResponse) -> None:
        if rsp is None:
            return
        step_down = False
        retry = False
        advance = False
        with self._lock:
            if rsp.term > self._cur_term:
                step_down = True
            elif self._role == self.LEADER:
                if rsp.success:
                    last_sent = self._last_append_request.get(
                        id, self._next_index.get(id, 0) - 1)
                    self._match_index[id] = max(
                        self._match_index.get(id, -1), last_sent)
                    self._next_index[id] = self._match_index[id] + 1
                    advance = True
                else:
                    if self._next_index.get(id, 0) > 0:
                        self._next_index[id] = self._next_index.get(id, 0) - 1
                        retry = True
        if step_down:
            self._step_down(rsp.term)
        elif advance:
            self._maybe_advance_commit()
        elif retry:
            self._send_append_entries_to(id)

    # ------------------------------------------------------------------
    # Election
    # ------------------------------------------------------------------
    def leader_elect_timeout_handler(self):
        with self._lock:
            if self._role == self.LEADER:
                return
            self._role = self.CANDIDATE
            self._vote_for = None
            self._granted = 0
            self._cur_term += 1
            self.persist_term_and_vote()
        logging.info("Start a new vote cycle for term: %s", self._cur_term)
        self.send_request_vote()
        self.restart_election_timer()

    def restart_election_timer(self):
        logging.debug("Restart election timer")
        if self._election_timer is not None:
            self._election_timer.cancel()
        self._election_timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._election_timer.daemon = True
        self._election_timer.start()

    @staticmethod
    def send_request_vote_message(ip: str, port: int, msg: raft_pb2.MsgVoteRequest) \
            -> raft_pb2.MsgVoteResponse:
        try:
            with grpc.insecure_channel(f'{ip}:{port}') as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                return stub.RequestVote(msg, timeout=RaftServicer.RPC_TIMEOUT)
        except Exception:
            logging.warning("Failed to send request vote to %s:%s", ip, port)
            return None

    def send_request_vote_to_all(self, msg: raft_pb2.MsgVoteRequest) -> None:
        with self._lock:
            if self._vote_for is None:
                self._granted += 1
                self._vote_for = self._id
            peers = list(self._peers)
        f_list = [self._rpc_executor.submit(
            self.send_request_vote_message, p.ip, p.port, msg) for p in peers]
        for f in futures.as_completed(f_list):
            try:
                resp = f.result()
            except Exception:
                continue
            self.handle_vote_response(resp)

    def send_request_vote(self):
        with self._lock:
            cur_term = self._cur_term
            last_log_index = len(self._log_entries) - 1
            last_log_term = self._log_entries[last_log_index].term \
                if last_log_index >= 0 else 0
            peers = list(self._peers)
        rpc_message = raft_pb2.MsgVoteRequest(term=cur_term, candidateId=self._id,
                                              lastLogIndex=last_log_index,
                                              lastLogTerm=last_log_term)
        if not peers:
            self._become_leader()
        else:
            self.send_request_vote_to_all(rpc_message)

    def handle_vote_response(self, resp: raft_pb2.MsgVoteResponse) -> None:
        if resp is None:
            return
        win = False
        with self._lock:
            if resp.term > self._cur_term:
                self._step_down(resp.term)
                return
            if resp.term != self._cur_term or self._role != self.CANDIDATE \
                    or not resp.voteGranted:
                return
            self._granted += 1
            logging.debug("%s granted me as leader", self._granted)
            if self._granted > (len(self._peers) + 1) / 2:
                win = True
        if win:
            self._become_leader()

    # ------------------------------------------------------------------
    # RPC handlers
    # ------------------------------------------------------------------
    def RequestVote(self, request, context):
        logging.info("Process vote request: %s", request)
        with self._lock:
            if request.term > self._cur_term:
                self._cur_term = request.term
                self._vote_for = None
                self._role = self.FOLLOWER
                self.persist_term_and_vote()
            resp = raft_pb2.MsgVoteResponse(term=self._cur_term, voteGranted=False)
            if request.term < self._cur_term:
                return resp
            if self._vote_for is not None and self._vote_for != request.candidateId:
                return resp
            term_in_log = self._log_entries[-1].term if self._log_entries else -1
            last_log_index = len(self._log_entries) - 1
            up_to_date = (request.lastLogTerm > term_in_log or
                          (request.lastLogTerm == term_in_log and
                           request.lastLogIndex >= last_log_index))
            if not up_to_date:
                return resp
            self._vote_for = request.candidateId
            resp.voteGranted = True
            self.persist_term_and_vote()
        if resp.voteGranted:
            self.restart_election_timer()
        return resp

    def AppendEntries(self, request: raft_pb2.MsgAppendEntriesRequest, context):
        logging.debug("Process append entries request: %s", request)
        term_in_msg = request.term
        if term_in_msg < self._cur_term:
            logging.warning("Term in AppendEntriesRequest: %s  from %s"
                            "< my own term: %s", term_in_msg, request.leaderId,
                            self._cur_term)
            return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=False)

        with self._lock:
            if term_in_msg > self._cur_term:
                self._cur_term = term_in_msg
                self._vote_for = None
                self.persist_term_and_vote()
            self._role = self.FOLLOWER
            self._leader_id = request.leaderId
        self._stop_heartbeat()
        self.restart_election_timer()

        with self._lock:
            my_last_log_index = len(self._log_entries) - 1
            prev_log_index_in_msg = request.prevLogIndex
            prev_log_term_in_msg = request.prevLogTerm
            if my_last_log_index < prev_log_index_in_msg:
                return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=False)
            if prev_log_index_in_msg >= 0 and \
                    self._log_entries[prev_log_index_in_msg].term != prev_log_term_in_msg:
                return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=False)
            if len(request.entries) > 0:
                self._log_entries = self._log_entries[:prev_log_index_in_msg + 1]
                self._log_entries.extend(
                    Entry(e.term, e.op, e.data) for e in request.entries)
                self.persist_entries()
            if request.leaderCommit > self._commit_index:
                self._commit_index = min(request.leaderCommit,
                                         len(self._log_entries) - 1)
            self._apply_committed()
        return raft_pb2.MsgAppendEntriesResponse(term=self._cur_term, success=True)

    def SubmitCommand(self, request: raft_pb2.ClientCommandRequest, context):
        with self._lock:
            if self._role != self.LEADER:
                return raft_pb2.ClientCommandResponse(
                    success=False, leaderId=self._leader_id or "")
            entry = Entry(term=self._cur_term, op=request.op, data=request.data)
            self._log_entries.append(entry)
            self.persist_entries()
            target_index = len(self._log_entries) - 1
        self._maybe_advance_commit()
        self.send_append_entries_to_all()
        deadline = time.monotonic() + self.COMMIT_TIMEOUT
        while time.monotonic() < deadline:
            with self._lock:
                if self._role != self.LEADER:
                    return raft_pb2.ClientCommandResponse(
                        success=False, leaderId=self._leader_id or "")
                if self._commit_index >= target_index:
                    key, _, _ = entry.data.partition(":")
                    return raft_pb2.ClientCommandResponse(
                        success=True, leaderId=self._id,
                        value=self._state.get(key, ""))
            time.sleep(0.05)
        logging.warning("Node %s timed out waiting for commit of entry %s",
                        self._id, entry)
        return raft_pb2.ClientCommandResponse(success=False, leaderId=self._id)


def serve(port, node_id, neighbors, cur_term=0, entries=None):
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

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, 0o755, exist_ok=True)

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, 0o755, exist_ok=True)
    log_dir = os.path.join(LOG_DIR, f'Node{args.node}.log')
    logging.basicConfig(
        handlers=[RotatingFileHandler(
        log_dir, maxBytes=2 * 1024 * 1024, backupCount=10)],
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)s [%(funcName)s] %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S')

    logging.info("Node run on %s:%s with id %s", node_ip, node_port, args.node)
    logging.info("Peer nodes: %s", peers)
    serve(node_port, args.node, peers)
