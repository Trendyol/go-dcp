package membership

import "github.com/Trendyol/go-dcp-client/membership/info"

type Membership interface {
	GetInfo() *info.Model
}
