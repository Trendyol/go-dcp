package membership

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type haMembership struct {
	info     *info.Model
	infoChan chan *info.Model
}

func (h *haMembership) GetInfo() *info.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func NewHaMembership(_ helpers.ConfigDCPGroupMembership, handler info.Handler) membership.Membership {
	ham := &haMembership{
		infoChan: make(chan *info.Model),
	}

	handler.Subscribe(func(new *info.Model) {
		ham.info = new
		go func() {
			ham.infoChan <- new
		}()
	})

	return ham
}
