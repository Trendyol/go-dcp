package kubernetes

import (
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

func NewHaMembership(_ *helpers.Config, handler membership.Handler) membership.Membership {
	ham := &haMembership{
		infoChan: make(chan *membership.Model),
	}

	handler.Subscribe(func(new *membership.Model) {
		ham.info = new
		go func() {
			ham.infoChan <- new
		}()
	})

	return ham
}
