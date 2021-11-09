import argparse
import configparser
import logging
import random
import socket
import socketserver
import threading

from logging.handlers import RotatingFileHandler

VOTE_REQUEST_TYPE = "VOTE_REQUEST"
VOTE_RESPONSE_TYPE = "VOTE_RESPONSE"
APPEND_ENTRY_REQUEST = "APPEND_ENTRY_REQUEST"
APPEND_ENTRY_RESPONSE = "APPEND_ENTRY_RESPONSE"


class VoteRequest:
    def __init__(self, term, candidate_id,
                 last_log_index, last_log_term):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def __repr__(self):
        return "{}:{}:{}:{}:{}".format(VOTE_REQUEST_TYPE,
                                       self.term,
                                       self.candidate_id,
                                       self.last_log_index,
                                       self.last_log_term)


class VoteResponse:
    def __init__(self, term, granted):
        self.term = term
        self.granted = granted

    def __repr__(self):
        return "{}:{}:{}".format(VOTE_RESPONSE_TYPE,
                                 self.term, self.granted)


class Entry:
    def __init__(self, term, op, data):
        self.term = term
        self.op = op
        self.data = data


class AppendEntryRequest:
    def __init__(self, term, leader_id, prev_log_index,
                 entries, leader_commit_index):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.entries = entries
        self.leader_commit_index = leader_commit_index

    def __repr__(self):
        return "{}:{}:{}:{}:{}:{}:{}".format(APPEND_ENTRY_REQUEST,
                                             self.term,
                                             self.leader_id,
                                             self.prev_log_index,
                                             len(self.entries),
                                             self.entries,
                                             self.leader_commit_index)


class AppendEntryResponse:
    def __init__(self, term, success):
        self.term = term
        self.success = success

    def __repr__(self):
        return "{}:{}:{}".format(APPEND_ENTRY_RESPONSE,
                                 self.term, self.success)


class RpcHandler(socketserver.StreamRequestHandler):
    def handle(self):
        data = self.rfile.readline().strip()
        logging.info("Received %s from %s", data, self.client_address)
        data_str = data.decode('utf-8')
        fields = data_str.split(":")
        type_str = fields[0]
        try:
            if type_str == VOTE_REQUEST_TYPE:
                # According to TcpServer implementation, self.server
                # in BaseRequestHandler will be instance of TcpServer.
                # As in this case, it is Node instance.
                self.server.process_vote_request(self.wfile, fields)
            elif type_str == VOTE_RESPONSE_TYPE:
                self.server.process_vote_response(self.wfile, fields)
        except:
            logging.exception("Failed to process %s", fields)


class Node(socketserver.TCPServer):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
    ELECTION_TIME_LOW = 1
    ELECTION_TIME_HIGH = 10

    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True,
                 role=FOLLOWER, node_id=0, neighbors=None):
        self._ip, self._port = server_address
        logging.info("server_address: %s", server_address)
        socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        self._role = role
        self._cur_term = 0
        self._id = node_id
        self._vote_for = None
        self._log_entries = []
        self._commit_index = 0
        self._last_applied = -1
        self._peers = neighbors
        self._timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._timer.start()

    def process_vote_request(self, out_file, msg_fields):
        term_in_request = int(msg_fields[1])
        resp = VoteResponse(-1, 0)
        term_in_log = self._log_entries[-1].term if self._log_entries else -1
        last_log_index = len(self._log_entries) - 1
        if term_in_request < self._cur_term:
            resp_msg = '%s' % resp
            out_file.write(resp_msg.encode('utf-8'))
        else:
            if self._vote_for is None and \
                    (int(msg_fields[4]) > term_in_log or
                     (int(msg_fields[4]) == term_in_log and
                      int(msg_fields[3]) >= last_log_index)):
                resp.granted = 1
                resp.term = int(msg_fields[4])
                resp_msg = '%s' % resp
                out_file.write(resp_msg.encode('utf-8'))
            else:
                resp_msg = '%s' % resp
                out_file.write(resp_msg.encode('utf-8'))

    def process_vote_response(self, out_file, msg_fields):
        granted_count = 0
        logging.info("Vote response: %s", msg_fields)
        _, _, granted = msg_fields
        if int(granted):
            granted_count += 1
        if granted_count > len(self._peers) / 2:
            # Win election, change role to leader
            self._role = Node.LEADER
            # Start to send AppendEntriesRequest to peer nodes

    def leader_elect_timeout_handler(self):
        self._role = self.CANDIDATE
        self.request_vote()
        self._timer.cancel()
        self._timer = threading.Timer(
            random.randint(self.ELECTION_TIME_LOW, self.ELECTION_TIME_HIGH),
            self.leader_elect_timeout_handler)
        self._timer.start()

    def request_vote(self):
        self._cur_term += 1
        last_log_index = len(self._log_entries) - 1
        if last_log_index > -1:
            last_log_term = self._log_entries[last_log_index].term
        else:
            last_log_term = 0
        rpc_message = '%s' % VoteRequest(self._cur_term, self._id,
                                         last_log_index,
                                         last_log_term)
        if self._peers is None:
            self._role = Node.LEADER
        else:
            for peer_ip, peer_port in self._peers:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        logging.info("Send vote request message: %s to %s:%s",
                                     rpc_message, peer_ip, peer_port)
                        s.connect((peer_ip, int(peer_port)))
                        s.sendall(rpc_message.encode('utf-8'))
                    except:
                        logging.exception("Failed to communicate with %s:%s",
                                          peer_ip, peer_port)

    def add_peer(self, peer_ip, peer_port):
        self._peers.append((peer_ip, peer_port))

    def append_log(self):
        logging.info("Append logs: %s", self._log_entries)
        self._timer.cancel()
        # Update its own term with the term in the AppendLogRequest


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

    logging.basicConfig(
        handlers=[RotatingFileHandler(
            'Node%d.log' %args.node, maxBytes=20 * 1024 * 1024, backupCount=10)],
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)s [%(funcName)s] %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S')

    logging.info("Node run on %s:%s with id %s", node_ip, node_port, args.node)
    logging.info("Peer nodes: %s", peers)
    with Node((node_ip, node_port), RpcHandler, True, Node.FOLLOWER,
              node_id=args.node, neighbors=peers) as n:
        n.serve_forever()
