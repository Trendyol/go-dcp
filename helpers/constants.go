package helpers

const Name = "cbgo"

const (
	Prefix string = "_connector:" + Name + ":"

	MembershipChangedBusEventName   string = "membershipChanged"
	PersistSeqNoChangedBusEventName string = "persistSeqNoChanged"

	JSONFlags uint32 = 50333696
)
