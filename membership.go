package main

import "log"

type Membership interface {
	GetVBuckets() []uint16
}

type membership struct {
	memberNumber  int
	totalMembers  int
	vBucketNumber int
}

func (s *membership) GetVBuckets() []uint16 {
	var vBuckets []uint16

	for i := 0; i < s.vBucketNumber; i++ {
		vBuckets = append(vBuckets, uint16(i))
	}

	readyToStreamVBuckets := ChunkSlice(vBuckets, s.totalMembers)[s.memberNumber-1]
	log.Printf("Ready to stream VBuckets range: %v-%v", readyToStreamVBuckets[0], readyToStreamVBuckets[len(readyToStreamVBuckets)-1])

	return readyToStreamVBuckets
}

func NewMembership(memberNumber int, totalMember int, vBucketNumber int) Membership {
	return &membership{
		memberNumber:  memberNumber,
		totalMembers:  totalMember,
		vBucketNumber: vBucketNumber,
	}
}
