package couchbase

import (
	"fmt"
	"strconv"
	"strings"
)

// Version
// snippet from https://github.com/couchbase/gocbcore/blob/master/nodeversion_test.go
type Version struct {
	Major int
	Minor int
	Patch int
	Build int
}

var (
	SrvVer501 = &Version{5, 0, 1, 0}
	SrvVer510 = &Version{5, 1, 0, 0}
	SrvVer550 = &Version{5, 5, 0, 0}
	SrvVer600 = &Version{6, 0, 0, 0}
	SrvVer650 = &Version{6, 5, 0, 0}
	SrvVer660 = &Version{6, 6, 0, 0}
	SrvVer700 = &Version{7, 0, 0, 0}
	SrvVer710 = &Version{7, 1, 0, 0}
	SrvVer720 = &Version{7, 2, 0, 0}
	SrvVer723 = &Version{7, 2, 3, 0}
)

func (v *Version) Equal(ov *Version) bool {
	if v.Major == ov.Major && v.Minor == ov.Minor &&
		v.Patch == ov.Patch && v.Build == ov.Build {
		return true
	}
	return false
}

func (v *Version) Higher(ov *Version) bool {
	if v.Major > ov.Major {
		return true
	} else if v.Major < ov.Major {
		return false
	}

	if v.Minor > ov.Minor {
		return true
	} else if v.Minor < ov.Minor {
		return false
	}

	if v.Patch > ov.Patch {
		return true
	} else if v.Patch < ov.Patch {
		return false
	}

	if v.Build > ov.Build {
		return true
	} else if v.Build < ov.Build {
		return false
	}

	return false
}

func (v *Version) Lower(ov *Version) bool {
	return !v.Higher(ov) && !v.Equal(ov)
}

func nodeVersionFromString(version string) (*Version, error) {
	vSplit := strings.Split(version, ".")
	lenSplit := len(vSplit)
	if lenSplit == 0 {
		return nil, fmt.Errorf("must provide at least a major version")
	}

	var err error
	nodeVersion := Version{}
	nodeVersion.Major, err = strconv.Atoi(vSplit[0])
	if err != nil {
		return nil, fmt.Errorf("major version is not a valid integer")
	}
	if lenSplit == 1 {
		return &nodeVersion, nil
	}

	nodeVersion.Minor, err = strconv.Atoi(vSplit[1])
	if err != nil {
		return nil, fmt.Errorf("minor version is not a valid integer")
	}
	if lenSplit == 2 {
		return &nodeVersion, nil
	}

	nodeBuild := strings.Split(vSplit[2], "-")
	nodeVersion.Patch, err = strconv.Atoi(nodeBuild[0])
	if err != nil {
		return nil, fmt.Errorf("patch version is not a valid integer")
	}
	if len(nodeBuild) == 1 {
		return &nodeVersion, nil
	}

	buildEdition := strings.Split(nodeBuild[1], "-")
	nodeVersion.Build, err = strconv.Atoi(buildEdition[0])
	if err != nil {
		return &nodeVersion, nil
	}
	if len(buildEdition) == 1 {
		return &nodeVersion, nil
	}

	return &nodeVersion, nil
}
