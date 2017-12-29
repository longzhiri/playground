package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"

	ErrMismatchView = "ErrMismatchView"

	ErrDuplicateOp = "ErrDuplicateOp"
	ErrViewChanged = "ErrViewChanged"
	ErrUnexpected  = "ErrUnexpected"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	OpType  string
	OpId    int64
	Primary string

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Primary string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type CloneArgs struct {
	Me          string
	AllData     map[string]string
	DoneOpIdMap map[int64]bool
	Viewnum     uint
}

type CloneReply struct {
	Err Err
}
