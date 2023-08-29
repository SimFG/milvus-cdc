// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	meta "github.com/zilliztech/milvus-cdc/server/model/meta"

	store "github.com/zilliztech/milvus-cdc/server/store"
)

// MetaStoreFactory is an autogenerated mock type for the MetaStoreFactory type
type MetaStoreFactory struct {
	mock.Mock
}

// GetTaskCollectionPositionMetaStore provides a mock function with given fields: ctx
func (_m *MetaStoreFactory) GetTaskCollectionPositionMetaStore(ctx context.Context) store.MetaStore[*meta.TaskCollectionPosition] {
	ret := _m.Called(ctx)

	var r0 store.MetaStore[*meta.TaskCollectionPosition]
	if rf, ok := ret.Get(0).(func(context.Context) store.MetaStore[*meta.TaskCollectionPosition]); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(store.MetaStore[*meta.TaskCollectionPosition])
		}
	}

	return r0
}

// GetTaskInfoMetaStore provides a mock function with given fields: ctx
func (_m *MetaStoreFactory) GetTaskInfoMetaStore(ctx context.Context) store.MetaStore[*meta.TaskInfo] {
	ret := _m.Called(ctx)

	var r0 store.MetaStore[*meta.TaskInfo]
	if rf, ok := ret.Get(0).(func(context.Context) store.MetaStore[*meta.TaskInfo]); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(store.MetaStore[*meta.TaskInfo])
		}
	}

	return r0
}

// Txn provides a mock function with given fields: ctx
func (_m *MetaStoreFactory) Txn(ctx context.Context) (interface{}, func(error) error, error) {
	ret := _m.Called(ctx)

	var r0 interface{}
	var r1 func(error) error
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context) (interface{}, func(error) error, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) interface{}); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) func(error) error); ok {
		r1 = rf(ctx)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(func(error) error)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context) error); ok {
		r2 = rf(ctx)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

type mockConstructorTestingTNewMetaStoreFactory interface {
	mock.TestingT
	Cleanup(func())
}

// NewMetaStoreFactory creates a new instance of MetaStoreFactory. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMetaStoreFactory(t mockConstructorTestingTNewMetaStoreFactory) *MetaStoreFactory {
	mock := &MetaStoreFactory{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
