package helpers

const Name = "cbgo"

const (
	Prefix    string = "_connector:" + Name + ":"
	TxnPrefix string = Prefix + "_txn:"

	MembershipChangedBusEventName   string = "membershipChanged"
	PersistSeqNoChangedBusEventName string = "persistSeqNoChanged"

	JSONFlags uint32 = 50333696
)
