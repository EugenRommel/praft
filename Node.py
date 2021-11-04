import logging
import socketserver

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


class Node(socketserver.StreamRequestHandler):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

    def __init__(self, request, client_address, server,
                 role, node_id, ip, port):
        super().__init__(request, client_address, server)
        self._role = role
        self._cur_term = 0
        self._id = node_id
        self._vote_for = None
        self._log_entries = []
        self._commit_index = 0
        self._last_applied = -1
        self._peers = list()
        self._ip = ip
        self._port = port

    def process_vote_request(self, msg_fields):
        term_in_request = int(msg_fields[1])
        resp = VoteResponse(-1, False)
        if term_in_request < self._cur_term:
            self.wfile.write(b'%s' % resp)
        else:
            if self._vote_for is None and \
                    (int(msg_fields[4]) > self._log_entries[-1].term or
                     (int(msg_fields[4]) == self._log_entries[-1].term and
                      int(msg_fields[3]) > len(self._log_entries) - 1)):
                resp.granted = True
                resp.term = int(msg_fields[4])
                self.wfile.write(b'%s' % resp)

    def handle(self) -> None:
        data = self.rfile.readline().strip()
        logging.info("Received %s from %s", data, self.client_address)
        fields = data.split(b":")
        type_str = fields[0]
        if type_str == VOTE_REQUEST_TYPE:
            self.process_vote_request(fields)

    def request_vote(self):
        last_log_index = len(self._log_entries) - 1
        if last_log_index > -1:
            last_log_term = self._log_entries[last_log_index].term
        else:
            last_log_term = 0
        rpc_message = VoteRequest(self._cur_term, self._id,
                                  last_log_index,
                                  last_log_term)
        logging.info("Request vote: %s", rpc_message)
        for ip, port in self._peers:
            self.wfile.write(b'%s\n' % rpc_message)

    def append_log(self):
        logging.info("Append logs: %s", self._log_entries)
