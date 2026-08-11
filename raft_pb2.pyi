from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MsgVoteRequest(_message.Message):
    __slots__ = ("term", "candidateId", "lastLogIndex", "lastLogTerm")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATEID_FIELD_NUMBER: _ClassVar[int]
    LASTLOGINDEX_FIELD_NUMBER: _ClassVar[int]
    LASTLOGTERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int
    def __init__(self, term: _Optional[int] = ..., candidateId: _Optional[str] = ..., lastLogIndex: _Optional[int] = ..., lastLogTerm: _Optional[int] = ...) -> None: ...

class MsgVoteResponse(_message.Message):
    __slots__ = ("term", "voteGranted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTEGRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    voteGranted: bool
    def __init__(self, term: _Optional[int] = ..., voteGranted: _Optional[bool] = ...) -> None: ...

class Entry(_message.Message):
    __slots__ = ("term", "op", "data")
    TERM_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    term: int
    op: str
    data: str
    def __init__(self, term: _Optional[int] = ..., op: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class MsgAppendEntriesRequest(_message.Message):
    __slots__ = ("term", "leaderId", "prevLogIndex", "prevLogTerm", "leaderCommit", "entries")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    PREVLOGINDEX_FIELD_NUMBER: _ClassVar[int]
    PREVLOGTERM_FIELD_NUMBER: _ClassVar[int]
    LEADERCOMMIT_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    leaderCommit: int
    entries: _containers.RepeatedCompositeFieldContainer[Entry]
    def __init__(self, term: _Optional[int] = ..., leaderId: _Optional[str] = ..., prevLogIndex: _Optional[int] = ..., prevLogTerm: _Optional[int] = ..., leaderCommit: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[Entry, _Mapping]]] = ...) -> None: ...

class MsgAppendEntriesResponse(_message.Message):
    __slots__ = ("term", "success")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    def __init__(self, term: _Optional[int] = ..., success: _Optional[bool] = ...) -> None: ...

class ClientCommandRequest(_message.Message):
    __slots__ = ("op", "data")
    OP_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    op: str
    data: str
    def __init__(self, op: _Optional[str] = ..., data: _Optional[str] = ...) -> None: ...

class ClientCommandResponse(_message.Message):
    __slots__ = ("success", "leaderId", "value")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    leaderId: str
    value: str
    def __init__(self, success: _Optional[bool] = ..., leaderId: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
