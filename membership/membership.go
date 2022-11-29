package membership

type Membership interface {
	GetVBuckets() []uint16
}
