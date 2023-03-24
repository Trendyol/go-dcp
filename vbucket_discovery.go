package godcpclient

import (
	"errors"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/helpers"
	kms "github.com/Trendyol/go-dcp-client/kubernetes/membership"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type VBucketDiscovery interface {
	Get() []uint16
	Close()
}

type vBucketDiscovery struct {
	membership    membership.Membership
	vBucketNumber int
}

func (s *vBucketDiscovery) Get() []uint16 {
	vBuckets := make([]uint16, 0, s.vBucketNumber)

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	receivedInfo := s.membership.GetInfo()

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, receivedInfo.TotalMembers)[receivedInfo.MemberNumber-1]
	logger.Log.Printf(
		"member: %v/%v, vbucket range: %v-%v",
		receivedInfo.MemberNumber,
		receivedInfo.TotalMembers,
		readyToStreamVBuckets[0],
		readyToStreamVBuckets[len(readyToStreamVBuckets)-1],
	)

	return readyToStreamVBuckets
}

func (s *vBucketDiscovery) Close() {
	s.membership.Close()
	logger.Log.Printf("vbucket discovery closed")
}

func NewVBucketDiscovery(client gDcp.Client,
	config *helpers.Config,
	vBucketNumber int,
	infoHandler info.Handler,
) VBucketDiscovery {
	var ms membership.Membership

	switch {
	case config.Dcp.Group.Membership.Type == helpers.StaticMembershipType:
		ms = membership.NewStaticMembership(config)
	case config.Dcp.Group.Membership.Type == helpers.CouchbaseMembershipType:
		ms = membership.NewCBMembership(config, client, infoHandler)
	case config.Dcp.Group.Membership.Type == helpers.KubernetesStatefulSetMembershipType:
		ms = kms.NewStatefulSetMembership(config)
	case config.Dcp.Group.Membership.Type == helpers.KubernetesHaMembershipType:
		ms = kms.NewHaMembership(config, infoHandler)
	default:
		err := errors.New("unknown membership")
		logger.ErrorLog.Printf("membership: %s, err: %v", config.Dcp.Group.Membership.Type, err)
		panic(err)
	}

	logger.Log.Printf("vbucket discovery opened with membership type: %s", config.Dcp.Group.Membership.Type)

	return &vBucketDiscovery{
		vBucketNumber: vBucketNumber,
		membership:    ms,
	}
}
