package couchbase

import (
	"context"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/couchbase/gocbcore/v10"
)

func CreateDocument(ctx context.Context,
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	id []byte,
	value []byte,
	flags uint32,
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error, 1)

	op, err := agent.Set(gocbcore.SetOptions{
		Key:            id,
		Value:          value,
		Flags:          flags,
		Deadline:       deadline,
		Expiry:         expiry,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func UpdateDocument(ctx context.Context,
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	id []byte,
	value []byte,
	expiry uint32,
	cas *gocbcore.Cas,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error, 1)

	mutateInOptions := gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpSetDoc,
				Value: value,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}
	if cas != nil {
		mutateInOptions.Cas = *cas
	}
	op, err := agent.MutateIn(mutateInOptions, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func DeleteDocument(ctx context.Context, agent *gocbcore.Agent, scopeName string, collectionName string, id []byte) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error, 1)

	op, err := agent.Delete(gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.DeleteResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func UpsertXattrs(ctx context.Context,
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	id []byte,
	path string,
	value []byte,
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error, 1)

	op, err := agent.MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
				Value: value,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func GetXattrs(ctx context.Context, agent *gocbcore.Agent, scopeName string, collectionName string, id []byte, path string) ([]byte, error) { //nolint:lll
	opm := NewAsyncOp(ctx)

	errorCh := make(chan error, 1)
	documentCh := make(chan []byte, 1)

	op, err := agent.LookupIn(gocbcore.LookupInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Flags: memd.SubdocFlagXattrPath,
				Path:  path,
			},
		},
		Deadline:       time.Now().Add(time.Second * 5),
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.LookupInResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result.Ops[0].Value
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func Get(ctx context.Context, agent *gocbcore.Agent, scopeName string, collectionName string, id []byte) (*gocbcore.GetResult, error) {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	errorCh := make(chan error, 1)
	documentCh := make(chan *gocbcore.GetResult, 1)

	op, err := agent.Get(gocbcore.GetOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.GetResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func CreatePath(ctx context.Context,
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	value []byte,
	flags memd.SubdocDocFlag,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error, 1)

	op, err := agent.MutateIn(gocbcore.MutateInOptions{
		Key:   id,
		Flags: flags,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: value,
				Path:  string(path),
			},
		},
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	err = <-ch

	return err
}
