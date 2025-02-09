package offset

import (
	"github.com/Trendyol/go-dcp/config"
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

func (l *OffsetLatestSeqNoInit) InitializeLatestSeqNo(LatestSeqNo uint64) uint64 {
	var latestSeqNo uint64
	if l.config.IsDcpModeFinite() {
		latestSeqNo = LatestSeqNo
	} else {
		latestSeqNo = 0xffffffffffffffff
	}
	return latestSeqNo
}
