package main

const (
	SnapshotMarkerName = iota
	MutationName
	DeletionName
	ExpirationName
	EndName
	CreateCollectionName
	DeleteCollectionName
	FlushCollectionName
	CreateScopeName
	DeleteScopeName
	ModifyCollectionName
	OSOSnapshotName
	SeqNoAdvancedName
)

type Listener func(int, interface{}, error)
