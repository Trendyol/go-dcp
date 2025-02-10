package helpers

const Name = "cbgo"

const (
	Prefix    string = "_connector:" + Name + ":"
	TxnPrefix string = "_txn:"

	MembershipChangedBusEventName   string = "membershipChanged"
	PersistSeqNoChangedBusEventName string = "persistSeqNoChanged"

	JSONFlags   uint32 = 50333696
	MaxIntValue uint64 = 0xffffffffffffffff
)
