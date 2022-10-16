package main

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

	return ChunkSlice(vBuckets, s.totalMembers)[s.memberNumber-1]
}

func NewMembership(memberNumber int, totalMember int, vBucketNumber int) Membership {
	return &membership{
		memberNumber:  memberNumber,
		totalMembers:  totalMember,
		vBucketNumber: vBucketNumber,
	}
}
