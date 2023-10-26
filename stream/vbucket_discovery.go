package stream

import (
	"errors"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/kubernetes"

	"github.com/Trendyol/go-dcp/couchbase"

	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/membership"
)

type VBucketDiscovery interface {
	Get() []uint16
	Close()
	GetMetric() *VBucketDiscoveryMetric
}

type vBucketDiscovery struct {
	membership             membership.Membership
	vBucketDiscoveryMetric *VBucketDiscoveryMetric
	vBucketNumber          int
}

type VBucketDiscoveryMetric struct {
	Type              string
	TotalMembers      int
	MemberNumber      int
	VBucketCount      int
	VBucketRangeStart uint16
	VBucketRangeEnd   uint16
}

func (s *vBucketDiscovery) Get() []uint16 {
	vBuckets := make([]uint16, 0, s.vBucketNumber)

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	receivedInfo := s.membership.GetInfo()

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, receivedInfo.TotalMembers)[receivedInfo.MemberNumber-1]

	start := readyToStreamVBuckets[0]
	end := readyToStreamVBuckets[len(readyToStreamVBuckets)-1]

	logger.Log.Info(
		"member: %v/%v, vbucket range: %v-%v",
		receivedInfo.MemberNumber, receivedInfo.TotalMembers,
		start, end,
	)

	s.vBucketDiscoveryMetric.TotalMembers = receivedInfo.TotalMembers
	s.vBucketDiscoveryMetric.MemberNumber = receivedInfo.MemberNumber
	s.vBucketDiscoveryMetric.VBucketRangeStart = start
	s.vBucketDiscoveryMetric.VBucketRangeEnd = end

	return readyToStreamVBuckets
}

func (s *vBucketDiscovery) Close() {
	s.membership.Close()
	logger.Log.Debug("vbucket discovery closed")
}

func (s *vBucketDiscovery) GetMetric() *VBucketDiscoveryMetric {
	return s.vBucketDiscoveryMetric
}

func NewVBucketDiscovery(client couchbase.Client,
	config *config.Dcp,
	vBucketNumber int,
	bus helpers.Bus,
) VBucketDiscovery {
	var ms membership.Membership

	switch {
	case config.Dcp.Group.Membership.Type == membership.StaticMembershipType:
		ms = membership.NewStaticMembership(config)
	case config.Dcp.Group.Membership.Type == membership.CouchbaseMembershipType:
		ms = couchbase.NewCBMembership(config, client, bus)
	case config.Dcp.Group.Membership.Type == membership.KubernetesStatefulSetMembershipType:
		ms = kubernetes.NewStatefulSetMembership(config)
	case config.Dcp.Group.Membership.Type == membership.KubernetesHaMembershipType:
		ms = kubernetes.NewHaMembership(config, bus)
	default:
		err := errors.New("unknown membership")
		logger.Log.Error("membership: %s, err: %v", config.Dcp.Group.Membership.Type, err)
		panic(err)
	}

	logger.Log.Debug("vbucket discovery opened with membership type: %s", config.Dcp.Group.Membership.Type)

	return &vBucketDiscovery{
		vBucketNumber: vBucketNumber,
		membership:    ms,
		vBucketDiscoveryMetric: &VBucketDiscoveryMetric{
			VBucketCount: vBucketNumber,
			Type:         config.Dcp.Group.Membership.Type,
		},
	}
}
