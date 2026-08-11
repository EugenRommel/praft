import argparse
import json
import sys

import grpc

import raft_pb2
import raft_pb2_grpc

MAX_REDIRECTS = 5
RPC_TIMEOUT = 5.0


def resolve_node(config, node_id):
    entry = config.get(str(node_id))
    if entry is None:
        return None
    return entry["ip"], entry["port"]


def submit_command(host, port, op, data, config, config_path):
    current = (host, port)
    resp = None
    for _ in range(MAX_REDIRECTS):
        with grpc.insecure_channel(f"{current[0]}:{current[1]}") as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            resp = stub.SubmitCommand(
                raft_pb2.ClientCommandRequest(op=op, data=data),
                timeout=RPC_TIMEOUT)
        if resp.success or not resp.leaderId:
            break
        resolved = resolve_node(config, resp.leaderId)
        if resolved is None:
            print(f"leader {resp.leaderId!r} not found in {config_path}")
            break
        current = resolved
    return resp


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="praft gRPC client")
    arg_parser.add_argument("command",
                            help="command to submit, e.g. 'set a:1'")
    arg_parser.add_argument("--host", default="localhost")
    arg_parser.add_argument("--port", type=int, default=50051)
    arg_parser.add_argument("--conf", default="config.json")
    args = arg_parser.parse_args()

    parts = args.command.split(None, 1)
    op = parts[0]
    data = parts[1] if len(parts) > 1 else ""

    try:
        with open(args.conf) as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"failed to load {args.conf}: {e}")
        sys.exit(1)

    resp = submit_command(args.host, args.port, op, data, config, args.conf)
    if resp.success:
        print(f"OK: {op} {data} -> {resp.value}")
    elif resp.leaderId:
        print(f"Not leader, redirect to leader {resp.leaderId}")
        sys.exit(2)
    else:
        print("Command failed: no leader available")
        sys.exit(1)
