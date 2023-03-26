package membership

type Membership interface {
	GetInfo() *Model
	Close()
}
