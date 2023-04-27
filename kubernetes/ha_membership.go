package kubernetes

import (
	"github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership"
)

type haMembership struct {
	info     *membership.Model
	infoChan chan *membership.Model
}

func (h *haMembership) GetInfo() *membership.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func (h *haMembership) Close() {
	close(h.infoChan)
}

func (h *haMembership) membershipChangedListener(event interface{}) {
	model := event.(*membership.Model)

	h.info = model
	go func() {
		h.infoChan <- model
	}()
}

func NewHaMembership(_ *config.Dcp, bus helpers.Bus) membership.Membership {
	ham := &haMembership{
		infoChan: make(chan *membership.Model),
	}

	bus.Subscribe(helpers.MembershipChangedBusEventName, ham.membershipChangedListener)

	return ham
}
