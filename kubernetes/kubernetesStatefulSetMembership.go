package kubernetes

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership"
	"log"
)

type kubernetesStatefulSetMembership struct {
	totalMembers  int
	vBucketNumber int
}

func (s *kubernetesStatefulSetMembership) GetVBuckets() []uint16 {
	statefulSetInfo, err := NewStatefulSetInfoFromHostname()

	if err != nil {
		panic(err)
	}

	memberNumber := statefulSetInfo.podOrdinal + 1

	if memberNumber > s.totalMembers {
		panic("memberNumber is greater than totalMembers")
	}

	var vBuckets []uint16

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, s.totalMembers)[memberNumber-1]
	log.Printf("ready to stream vBuckets range: %v-%v", readyToStreamVBuckets[0], readyToStreamVBuckets[len(readyToStreamVBuckets)-1])

	return readyToStreamVBuckets
}

func NewKubernetesStatefulSetMembership(totalMember int, vBucketNumber int) membership.Membership {
	return &kubernetesStatefulSetMembership{
		totalMembers:  totalMember,
		vBucketNumber: vBucketNumber,
	}
}
