package kvsrv

type MessageType int

const (
	Modify MessageType = iota
	Report
)

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	MessageType MessageType
	MessageID   int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
