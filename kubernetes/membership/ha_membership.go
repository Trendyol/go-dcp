package membership

import (
	"time"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type haMembership struct {
	info *info.Model
}

func (h *haMembership) GetInfo() *info.Model {
	for {
		if h.info != nil {
			return h.info
		}

		time.Sleep(1 * time.Second)
	}
}

func NewHaMembership(_ helpers.ConfigDCPGroupMembership, handler info.Handler) membership.Membership {
	ham := &haMembership{}

	handler.Subscribe(func(new *info.Model) {
		ham.info = new
	})

	return ham
}
