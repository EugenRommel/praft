import argparse
import configparser
import logging
import random
import signal
import socket
import socketserver
import threading
import concurrent.futures

from logging.handlers import RotatingFileHandler
from Common import VOTE_REQUEST_TYPE, VOTE_RESPONSE_TYPE, \
    APPEND_ENTRY_REQUEST, APPEND_ENTRY_RESPONSE, \
    CLIENT_COMMAND_REQUEST, CLIENT_COMMAND_RESPONSE, \
    CLIEN_SUBCOMMAND_QURERY_LEADER


class VoteRequest:
    def __init__(self, sender_id, term, candidate_id,
                 last_log_index, last_log_term):
        self.sender = sender_id
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def __repr__(self):
        return "{}:{}:{}:{}:{}:{}".format(VOTE_REQUEST_TYPE,
                                          self.sender,
                                          self.term,
                                          self.candidate_id,
                                          self.last_log_index,
                                          self.last_log_term)


class VoteResponse:
    def __init__(self, sender, term, granted):
        self.sender = sender
        self.term = term
        self.granted = granted

    def __repr__(self):
        return "{}:{}:{}:{}".format(VOTE_RESPONSE_TYPE,
                                    self.sender, self.term,
                                    self.granted)


class Entry:
    def __init__(self, term, op, data):
        self.term = term
        self.op = op
        self.data = data

    def __repr__(self):
        return "{}:{}:{}".format(self.term, self.op, self.data)


class AppendEntryRequest:
    def __init__(self, term, leader_id, prev_log_index, prev_log_term,
                 entries, leader_commit_index):
        self.leader_id = leader_id
        self.term = term
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit_index = leader_commit_index

    def __repr__(self):
        return "{}:{}:{}:{}:{}:{}:{}".format(APPEND_ENTRY_REQUEST,
                                             self.leader_id,
                                             self.term,
                                             self.prev_log_index,
                                             self.prev_log_term,
                                             len(self.entries),
                                             self.entries,
                                             self.leader_commit_index)


class AppendEntryResponse:
    def __init__(self, sender, term, success):
        self.sender = sender
        self.term = term
        self.success = success

    def __repr__(self):
        return "{}:{}:{}:{}".format(APPEND_ENTRY_RESPONSE,
                                    self.sender,
                                    self.term, self.success)


class QueryLeaderResponse:
    def __init__(self, leader, success):
        self.leader = leader
        self.success = success

    def __repr__(self):
        return "{}:{}:{}".format(CLIENT_COMMAND_RESPONSE,
                                 CLIEN_SUBCOMMAND_QURERY_LEADER,
                                 self.leader, self.success)


class RpcHandler(socketserver.StreamRequestHandler):
    def handle(self):
        data = self.rfile.readline().strip()
        logging.info("Received %s from %s", data, self.client_address)
        data_str = data.decode('utf-8')
        fields = data_str.split(":")
        type_str = fields[0]
        sender = fields[1]
        logging.info("Type: %s, sender: node %s", type_str, sender)
        try:
            if type_str == VOTE_REQUEST_TYPE:
                # According to TcpServer implementation, self.server
                # in BaseRequestHandler will be an instance of TcpServer.
                # As in this case, it is Node instance.
                self.server.process_vote_request(fields)
            elif type_str == VOTE_RESPONSE_TYPE:
                self.server.process_vote_response(fields)
            elif type_str == APPEND_ENTRY_REQUEST:
                self.server.process_append_entries_request(fields)
            elif type_str == APPEND_ENTRY_RESPONSE:
                self.server.process_append_entries_response(fields)
            elif type_str == CLIENT_COMMAND_REQUEST:
                self.server.process_client_command(self.request)
            else:
                raise Exception("Unknown rpc type: %s" % type_str)
        except Exception:
            logging.exception("Failed to process %s", fields)


class NodeStatus:
    def __init__(self, id, ip, port, health):
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


class Node(socketserver.ThreadingTCPServer):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
    HB_TIME = 2
    ELECTION_TIME_LOW = 4 * HB_TIME
    ELECTION_TIME_HIGH = 8 * HB_TIME
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"

    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True,
                 role=FOLLOWER, node_id=0, neighbors=None, cur_term=0,
                 log_entries=None):
        self._ip, self._port = server_address
        logging.info("server_address: %s", server_address)
        socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        self._role = role
        self._cur_term = cur_term
        self._id = node_id
        self._vote_for = None
        if log_entries is None:
            self._log_entries = []
        else:
            self._log_entries = log_entries
        self._commit_index = 0
        self._last_applied = -1
        self._peers = {}
        self._granted = 0
        for peer in neighbors:
            # neighbors passed in is (id, ip, port) tuple
            # _peers is a dict with key=id, value=(ip, port)
            self._peers[peer[0]] = NodeStatus(*peer)
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
        self._data_file = "config.ini"

    def process_vote_request(self, msg_fields):
        term_in_request = int(msg_fields[1])
        resp = VoteResponse(self._id, -1, 0)
        term_in_log = self._log_entries[-1].term if self._log_entries else -1
        last_log_index = len(self._log_entries) - 1
        logging.info("Process vote request: %s", msg_fields)
        print("Vote for: %s, term_in_msg: %s, term_in_log: %s"
              ", index_in_msg: %s, last_log_index: %s" %
              (self._vote_for, int(msg_fields[4]), term_in_log,
               int(msg_fields[3]), last_log_index))
        if term_in_request < self._cur_term:
            resp_msg = '%s' % resp
        else:
            if self._vote_for is None and \
                    (int(msg_fields[4]) > term_in_log or
                     (int(msg_fields[4]) == term_in_log and
                      int(msg_fields[3]) >= last_log_index)):
                resp.granted = 1
                self._vote_for = int(msg_fields[1])
                resp.term = int(msg_fields[4])
                resp_msg = '%s' % resp
            else:
                resp_msg = '%s' % resp
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                peer_id = int(msg_fields[1])
                if peer_id not in self._peers:
                    logging.error("Unknown node %s", peer_id)
                    return
                peer_ip, peer_port = \
                    self._peers[peer_id].ip, self._peers[peer_id].port
                logging.info("Send vote response message: %s to node %s"
                             " with address %s",
                             resp_msg, peer_id, (peer_ip, peer_port))
                s.connect((peer_ip, peer_port))
                s.sendall(resp_msg.encode('utf-8'))
            except Exception:
                logging.exception("Failed to send response to node %s",
                                  peer_id)

    def send_rpc_message_to_all(self, rpc_message):
        for peer_id, peer_node in self._peers.items():
            if peer_node.is_healthy():
                self.mark_peer_health(peer_id, self.HEALTHY)
            else:
                self.mark_peer_health(peer_id, self.UNHEALTHY)
            num_worker = min(len(self._peers), 4)
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_worker) as executor:
                executor.submit(self.send_rpc_message_to_node, peer_id, rpc_message)

    def send_rpc_message_to_node(self, node, rpc_msg):
        peer_ip, peer_port = self._peers[node].ip, self._peers[node].port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                logging.info("Send rpc message: %s to node %s"
                             " with address %s:%s",
                             rpc_msg, node, peer_ip, peer_port)
                s.connect((peer_ip, peer_port))
                s.sendall(rpc_msg.encode('utf-8'))
            except Exception:
                logging.exception("Failed to communicate with node %s"
                                  " with address %s:%s",
                                  node, peer_ip, peer_port)
            finally:
                self._peers[node].timeout()

    def heartbeat_nodes(self):
        prev_log_index = len(self._log_entries) - 1
        prev_log_term = \
            -1 if prev_log_index else self._log_entries[prev_log_index].term
        rpc = AppendEntryRequest(self._cur_term, self._id, prev_log_index,
                                 prev_log_term, [], self._commit_index)
        rpc_msg = "%s" % rpc
        self.send_rpc_message_to_all(rpc_msg)
        logging.info("Node %s heart beating follower nodes: %s",
                     self._id, rpc_msg)
        # Start heart beat timer
        self._hb_timer = threading.Timer(self.HB_TIME,
                                         self.heartbeat_nodes)
        self._hb_timer.start()

    def process_vote_response(self, msg_fields):
        logging.info("Vote response: %s", msg_fields)
        _, id, term_in_msg, granted = msg_fields
        self._peers[int(id)].reset_timeout()
        if int(granted):
            self._granted += 1
        # Grant self if my term is bigger
        if self._cur_term > int(term_in_msg):
            self._granted += 1
        logging.debug("Granted me as leader: %s", self._granted)
        healthy_peers = 0
        for id, status in self._peers.items():
            healthy_peers += status.is_healthy()
        if self._granted > healthy_peers / 2:
            # Win election, change role to leader
            self._role = Node.LEADER
            self._leader_id = self._id
            logging.info("I am the leader")
            print("I am leader")
            # Start to send AppendEntriesRequest to peer nodes
            last_log_index = len(self._log_entries) - 1
            last_log_term = \
                -1 if last_log_index == -1 \
                    else self._log_entries[last_log_index].term
            rpc_msg = AppendEntryRequest(self._cur_term, self._id,
                                         last_log_index, last_log_term,
                                         [], self._commit_index)
            self.send_rpc_message_to_all("%s" % rpc_msg)
            # Start heart beat timer
            self._hb_timer = threading.Timer(self.HB_TIME,
                                             self.heartbeat_nodes)
            self._hb_timer.start()

            # stop election timer
            if self._election_timer:
                self._election_timer.cancel()

    def restart_election_timer(self):
        logging.info("Restart election timer")
        self._election_timer.cancel()
        self._election_timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._election_timer.start()

    def process_append_entries_request(self, msg_fields):
        logging.info("Process append entries request: %s", msg_fields)
        # AppendEntriesRequest message fields:
        # APPEND_ENTRY_REQUEST:leader_id:term:prev_log_index:
        # prev_log_term:entries count:entry[0]...:commit_log_index
        term_in_msg = int(msg_fields[2])
        resp = AppendEntryResponse(self._id, self._cur_term, 0)
        leader_id = int(msg_fields[1])
        if term_in_msg < self._cur_term:
            logging.warning("Term in AppendEntriesRequest: %s  from %s"
                            "< my own term: %s", term_in_msg, leader_id,
                            self._cur_term)
            self.send_rpc_message_to_node(leader_id, "%s" % resp)
            return

        self._role = self.FOLLOWER
        self._leader_id = leader_id
        if self._hb_timer is not None:
            self._hb_timer.cancel()
            self._hb_timer = None
        self.restart_election_timer()
        my_last_log_index = len(self._log_entries) - 1
        my_last_log_term = \
            -1 if my_last_log_index == -1 else self._log_entries[
                my_last_log_index]
        prev_log_index_in_msg = int(msg_fields[3])
        prev_log_term_in_msg = int(msg_fields[4])
        if my_last_log_index < prev_log_index_in_msg:
            self.send_rpc_message_to_node(leader_id, "%s" % resp)
            return
        if my_last_log_index != prev_log_index_in_msg or \
                my_last_log_term != prev_log_term_in_msg:
            self._log_entries = self._log_entries[:prev_log_index_in_msg]
            self.send_rpc_message_to_node(leader_id, "%s" % resp)
            return
        resp.success = 1
        append_count = int(msg_fields[5])
        if append_count:
            # Remove '[' at the beginning and ']' at the end
            entry_string = msg_fields[6][1:-1]
            entry_list = entry_string.split(',')
            for entry in entry_list:
                # process entry: term:op:data
                term_in_entry, op, data = entry.split(':')
                entry_to_append = Entry(int(term_in_entry), op, data)
                self._log_entries.append(entry_to_append)
        self.send_rpc_message_to_node(leader_id, "%s" % resp)
        return

    def process_append_entries_response(self, msg_fields):
        # AppendEntriesResponse fields
        # APPEND_ENTRY_RESPONSE:sender id: sender term: success
        _, sender, _, success = msg_fields
        sender = int(sender)
        self._peers[sender].reset_timeout()
        success = int(success)
        if not success:
            prev_log_index = \
                self._nodes_prev_index.setdefault(
                    sender, len(self._log_entries) - 1) - 1
            prev_log_term = -1 if not self._log_entries else \
                self._log_entries[prev_log_index].term
            self._nodes_prev_index[sender] = prev_log_index
            rpc = AppendEntryRequest(self._cur_term,
                                     self._id,
                                     prev_log_index,
                                     prev_log_term,
                                     self._log_entries[prev_log_index + 1:],
                                     self._commit_index)
            self.send_rpc_message_to_node(sender, "%s" % rpc)

    def persist_term(self):
        config_parser = configparser.ConfigParser()
        config_parser.read([self._data_file])
        config_parser.set("Node%d" % self._id, "term",
                          "%d" % self._cur_term)
        with open(self._data_file, 'w+') as out:
            config_parser.write(out)

    def leader_elect_timeout_handler(self):
        logging.info("Start a new vote cycle for term: %s",
                     self._cur_term)
        if self._role == self.LEADER:
            logging.warning("I am NOT leader any more")
            print("I am NOT leader any more")
        self._role = self.CANDIDATE
        self._vote_for = None
        self._granted = 0
        self._cur_term += 1
        self.request_vote()
        self.restart_election_timer()
        self.persist_term()

    def request_vote(self):
        last_log_index = len(self._log_entries) - 1
        if last_log_index > -1:
            last_log_term = self._log_entries[last_log_index].term
        else:
            last_log_term = 0
        rpc_message = '%s' % VoteRequest(self._id, self._cur_term, self._id,
                                         last_log_index,
                                         last_log_term)
        if not self._peers:
            self._role = Node.LEADER
        else:
            self.send_rpc_message_to_all(rpc_message)

    def add_peer(self, peer_id, peer_ip, peer_port, health):
        self._peers[peer_id] = NodeStatus(peer_id, peer_ip,
                                          peer_port, health)

    def mark_peer_health(self, peer_id, health):
        config_parser = configparser.ConfigParser()
        config_parser.read([self._data_file])
        section = "Node%d" % self._id
        print("Node %s: %s" % (peer_id, self._peers[peer_id].health))
        peer_nodes = config_parser[section]['peers'].split(',')
        peer_list = [p.split(':') for p in peer_nodes]
        for p in peer_list:
            if p[0] == str(peer_id):
                p[3] = health
        peers_value = ','.join([':'.join(p) for p in peer_list])
        config_parser.set(section_name, "peers", peers_value)
        with open(self._data_file, 'w+') as out:
            config_parser.write(out)

    def process_client_command(self, connection):
        success = 0
        if self._leader_id is not None:
            success = 1
        rpc_resp = QueryLeaderResponse(self._leader_id, success)
        resp_msg = "%s" % rpc_resp
        logging.debug("Responding with %s", resp_msg)
        try:
            connection.sendall(resp_msg.encode('UTF-8'))
        except:
            logging.exception("Failed to respond client query")
        logging.info("Responded with %s", resp_msg)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--conf", help="config file path", default='config.ini')
    # Node id is an integer. It requires there is corresponding
    # section 'Node'+ id exists in config file specified by
    # --conf option. For example, if node id is 0, there should
    # be a section 'Node0' in config file
    arg_parser.add_argument("--node", type=int, help="id of node")
    args = arg_parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.conf)
    section_name = 'Node{}'.format(args.node)
    if section_name not in config:
        print("Node {} config not found in {}".format(args.node, args.conf))
        exit(-1)
    node_ip = config[section_name]['ip'].strip()
    node_port = int(config[section_name]['port'])
    node_peers = config[section_name]['peers'].split(',')
    peers = [p.split(':') for p in node_peers]
    peers = [(int(p[0]), p[1], int(p[2]), p[3]) for p in peers]
    term = int(config[section_name]['term'])
    entries = config[section_name].get('entries', None)

    logging.basicConfig(
        handlers=[RotatingFileHandler(
            'Node%d.log' % args.node, maxBytes=2 * 1024 * 1024, backupCount=10)],
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)s [%(funcName)s] %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S')

    logging.info("Node run on %s:%s with id %s", node_ip, node_port, args.node)
    logging.info("Peer nodes: %s", peers)
    with Node((node_ip, node_port), RpcHandler, True, Node.FOLLOWER,
              node_id=args.node, neighbors=peers, cur_term=term,
              log_entries=entries) as n:
        n.serve_forever()
