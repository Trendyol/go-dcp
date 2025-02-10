package offset

import (
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
)

type OffsetLatestSeqNoInit struct {
	config *config.Dcp
}

func NewOffsetLatestSeqNoInit(
	config *config.Dcp,
) *OffsetLatestSeqNoInit {
	return &OffsetLatestSeqNoInit{
		config,
	}
}

func (l *OffsetLatestSeqNoInit) InitializeLatestSeqNo(vBucketSeqNo uint64) uint64 {
	var latestSeqNo uint64
	if l.config.IsDcpModeFinite() {
		latestSeqNo = vBucketSeqNo
	} else {
		latestSeqNo = helpers.MaxIntValue
	}
	return latestSeqNo
}
