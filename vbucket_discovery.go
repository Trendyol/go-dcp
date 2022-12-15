package godcpclient

import (
	"fmt"
	"log"

	"github.com/Trendyol/go-dcp-client/helpers"
	kms "github.com/Trendyol/go-dcp-client/kubernetes/membership"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type VBucketDiscovery interface {
	Get() []uint16
}

type vBucketDiscovery struct {
	vBucketNumber int
	config        helpers.ConfigDCPGroupMembership
	infoHandler   info.Handler
	membership    membership.Membership
}

func (s *vBucketDiscovery) Get() []uint16 {
	var vBuckets []uint16

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	receivedInfo := s.membership.GetInfo()

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, receivedInfo.TotalMembers)[receivedInfo.MemberNumber-1]
	log.Printf(
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
		panic(fmt.Errorf("unknown membership type %s", config.Type))
	}

	return &vBucketDiscovery{
		vBucketNumber: vBucketNumber,
		config:        config,
		infoHandler:   infoHandler,
		membership:    ms,
	}
}
