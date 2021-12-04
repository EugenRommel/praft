import argparse
import socket

AVAILABLE_COMMANDS = ("query_leader",)


def query_leader(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(b"QUERY_LEADER_REQUEST:-1\n")
        print("Query sent")
        data = s.recv(1024)
        print('Received %s' % data)
        _, leader_id, success = data.decode('UTF-8').split(":")
        if not int(success):
            print('No leader found')
        else:
            print('Leader is %s' % leader_id)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        usage="Client.py <cmd> --host <host_ip> --port <port>",
        description="praft client")
    arg_parser.add_argument("command",
                            help="command to execute"
                            ",available command: query_leader")
    arg_parser.add_argument("--host", default="localhost")
    arg_parser.add_argument("--port", type=int, default=9996)
    args = arg_parser.parse_args()
    print("Command: %s to host: %s:%s"
          % (args.command, args.host, args.port))
    if args.command == "query_leader":
        query_leader(args.host, args.port)
    else:
        print("Unknown command, available commands: %s" \
              % AVAILABLE_COMMANDS)

