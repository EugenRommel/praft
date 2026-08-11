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

### Submit commands from a client

Once a leader is elected, submit commands through any node — the node
redirects you to the leader if needed:

```bash
python Client.py "set a:1" --port 50051   # OK: set a:1 -> 1
python Client.py "set b:2" --port 50052   # follows redirects to the leader
```

The client resolves the leader's address from `--conf config.json`
(default). The command format is `<op> <data>`; `set` stores
`key:value` pairs in the replicated state machine.

### Log compaction / snapshots

Leaders snapshot the state machine once `commit_index` advances past the
snapshot threshold (`SNAPSHOT_THRESHOLD` in `Node.py`, default 10000
entries). Compacted log prefixes are replaced by a snapshot (last included
index/term + serialized state) persisted in `data/NodeN.json`. A follower
that falls behind the compaction point is brought up to date with the
`InstallSnapshot` RPC instead of replaying the log.

### Tests

```bash
python -m unittest test_smoke   # election, replication, commit, client command, snapshots
```

