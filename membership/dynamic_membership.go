package membership

import (
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
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
	logger.Log.Info("dynamic membership waiting first request")
	return <-d.infoChan
}

func (d *dynamicMembership) Close() {
	err := d.bus.Unsubscribe(helpers.MembershipChangedBusEventName, d.membershipChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}
}

func (d *dynamicMembership) membershipChangedListener(m *Model) {
	shouldSendMessage := d.info == nil
	d.info = m
	if shouldSendMessage {
		go func() {
			d.infoChan <- m
		}()
	}
}

func NewDynamicMembership(bus EventBus.Bus) Membership {
	dm := &dynamicMembership{
		infoChan: make(chan *Model),
		bus:      bus,
	}

	err := bus.SubscribeAsync(helpers.MembershipChangedBusEventName, dm.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("error while subscribe membership changed event, err: %v", err)
		panic(err)
	}

	return dm
}
