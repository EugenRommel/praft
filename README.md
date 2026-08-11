# praft
Implement RAFT in python to demonstrate raft algorithm

## How to use

### Setup

With [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv sync                    # installs grpcio/grpcio-tools into .venv
uv run python --version    # activate .venv implicitly
```

Or with pip:

```bash
pip install -r requirements.txt
```

### Generate protobuf stubs

Run once (or after editing `proto/raft.proto`):

```bash
python -m grpc_tools.protoc -I proto \
  --python_out=. --grpc_python_out=. --pyi_out=. proto/raft.proto
```

This produces `raft_pb2.py` and `raft_pb2_grpc.py` in the repo root.

### Start 3 nodes

`config.json` defines 3 nodes (ids `1`, `2`, `3`) on ports 50051-50053:

```bash
python Node.py --node 1 &
python Node.py --node 2 &
python Node.py --node 3 &
```

Each node persists its term/vote/log in `data/NodeN.json` and logs to
`log/NodeN.log`. Check each node's log to find out how leader is elected.

