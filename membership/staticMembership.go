package membership

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"log"
)

type staticMembership struct {
	memberNumber  int
	totalMembers  int
	vBucketNumber int
}

func (s *staticMembership) GetVBuckets() []uint16 {
	var vBuckets []uint16

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	readyToStreamVBuckets := helpers.ChunkSlice[uint16](vBuckets, s.totalMembers)[s.memberNumber-1]
	log.Printf("ready to stream vBuckets range: %v-%v", readyToStreamVBuckets[0], readyToStreamVBuckets[len(readyToStreamVBuckets)-1])

	return readyToStreamVBuckets
}

func NewStaticMembership(memberNumber int, totalMember int, vBucketNumber int) Membership {
	return &staticMembership{
		memberNumber:  memberNumber,
		totalMembers:  totalMember,
		vBucketNumber: vBucketNumber,
	}
}
