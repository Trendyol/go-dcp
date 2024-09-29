package membership

import (
	"github.com/asaskevich/EventBus"
)

type dynamicMembership struct {
	info     *Model
	infoChan chan *Model
	bus      EventBus.Bus
}

func (d *dynamicMembership) GetInfo() *Model {
	if d.info != nil {
		return d.info
	}
	return <-d.infoChan
}

func (d *dynamicMembership) SetInfo(m *Model) {
	shouldSendMessage := d.info == nil
	d.info = m

	if shouldSendMessage {
		go func() {
			d.infoChan <- m
		}()
	}
}

func (d *dynamicMembership) Close() {}

func NewDynamicMembership() Membership {
	dm := &dynamicMembership{
		infoChan: make(chan *Model),
	}

	return dm
}
