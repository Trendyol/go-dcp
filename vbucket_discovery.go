package godcpclient

import (
	"fmt"

	"github.com/Trendyol/go-dcp-client/helpers"
	kms "github.com/Trendyol/go-dcp-client/kubernetes/membership"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type VBucketDiscovery interface {
	Get() []uint16
}

type vBucketDiscovery struct {
	infoHandler   info.Handler
	membership    membership.Membership
	config        helpers.ConfigDCPGroupMembership
	vBucketNumber int
}

func (s *vBucketDiscovery) Get() []uint16 {
	vBuckets := make([]uint16, 0, s.vBucketNumber)

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	receivedInfo := s.membership.GetInfo()

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, receivedInfo.TotalMembers)[receivedInfo.MemberNumber-1]
	logger.Debug(
		"ready to stream member number: %v, vBuckets range: %v-%v",
		receivedInfo.MemberNumber,
		readyToStreamVBuckets[0],
		readyToStreamVBuckets[len(readyToStreamVBuckets)-1],
	)

	return readyToStreamVBuckets
}

func NewVBucketDiscovery(config helpers.ConfigDCPGroupMembership, vBucketNumber int, infoHandler info.Handler) VBucketDiscovery {
	var ms membership.Membership

	switch {
	case config.Type == helpers.StaticMembershipType:
		ms = membership.NewStaticMembership(config)
	case config.Type == helpers.KubernetesStatefulSetMembershipType:
		ms = kms.NewStatefulSetMembership(config)
	case config.Type == helpers.KubernetesHaMembershipType:
		ms = kms.NewHaMembership(config, infoHandler)
	default:
		logger.Panic(fmt.Errorf("unknown membership"), "membership: %s", config.Type)
	}

	return &vBucketDiscovery{
		vBucketNumber: vBucketNumber,
		config:        config,
		infoHandler:   infoHandler,
		membership:    ms,
	}
}
