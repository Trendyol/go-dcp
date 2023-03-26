package membership

type Model struct {
	MemberNumber int
	TotalMembers int
}

func (s *Model) IsChanged(other *Model) bool {
	if other == nil {
		return true
	}

	return s.MemberNumber != other.MemberNumber || s.TotalMembers != other.TotalMembers
}
