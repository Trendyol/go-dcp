package kubernetes

import (
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/membership"
	"github.com/asaskevich/EventBus"
)

type haMembership struct {
	info     *membership.Model
	infoChan chan *membership.Model
	bus      EventBus.Bus
}

func (h *haMembership) GetInfo() *membership.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func (h *haMembership) Close() {
	err := h.bus.Unsubscribe(helpers.MembershipChangedBusEventName, h.membershipChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}
}

func (h *haMembership) membershipChangedListener(model *membership.Model) {
	shouldSendMessage := h.info == nil
	h.info = model
	if shouldSendMessage {
		go func() {
			h.infoChan <- model
		}()
	}
}

func NewHaMembership(_ *config.Dcp, bus EventBus.Bus) membership.Membership {
	ham := &haMembership{
		infoChan: make(chan *membership.Model),
		bus:      bus,
	}

	err := bus.SubscribeAsync(helpers.MembershipChangedBusEventName, ham.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("error while subscribe membership changed event, err: %v", err)
		panic(err)
	}

	return ham
}
