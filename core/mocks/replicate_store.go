// Code generated by mockery v2.32.4. DO NOT EDIT.

package mocks

import (
	context "context"

	api "github.com/zilliztech/milvus-cdc/core/api"

	mock "github.com/stretchr/testify/mock"
)

// ReplicateStore is an autogenerated mock type for the ReplicateStore type
type ReplicateStore struct {
	mock.Mock
}

type ReplicateStore_Expecter struct {
	mock *mock.Mock
}

func (_m *ReplicateStore) EXPECT() *ReplicateStore_Expecter {
	return &ReplicateStore_Expecter{mock: &_m.Mock}
}

// Get provides a mock function with given fields: ctx, key, withPrefix
func (_m *ReplicateStore) Get(ctx context.Context, key string, withPrefix bool) ([]api.MetaMsg, error) {
	ret := _m.Called(ctx, key, withPrefix)

	var r0 []api.MetaMsg
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, bool) ([]api.MetaMsg, error)); ok {
		return rf(ctx, key, withPrefix)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, bool) []api.MetaMsg); ok {
		r0 = rf(ctx, key, withPrefix)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]api.MetaMsg)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, bool) error); ok {
		r1 = rf(ctx, key, withPrefix)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplicateStore_Get_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Get'
type ReplicateStore_Get_Call struct {
	*mock.Call
}

// Get is a helper method to define mock.On call
//  - ctx context.Context
//  - key string
//  - withPrefix bool
func (_e *ReplicateStore_Expecter) Get(ctx interface{}, key interface{}, withPrefix interface{}) *ReplicateStore_Get_Call {
	return &ReplicateStore_Get_Call{Call: _e.mock.On("Get", ctx, key, withPrefix)}
}

func (_c *ReplicateStore_Get_Call) Run(run func(ctx context.Context, key string, withPrefix bool)) *ReplicateStore_Get_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(bool))
	})
	return _c
}

func (_c *ReplicateStore_Get_Call) Return(_a0 []api.MetaMsg, _a1 error) *ReplicateStore_Get_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ReplicateStore_Get_Call) RunAndReturn(run func(context.Context, string, bool) ([]api.MetaMsg, error)) *ReplicateStore_Get_Call {
	_c.Call.Return(run)
	return _c
}

// Put provides a mock function with given fields: ctx, key, value
func (_m *ReplicateStore) Put(ctx context.Context, key string, value api.MetaMsg) error {
	ret := _m.Called(ctx, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, api.MetaMsg) error); ok {
		r0 = rf(ctx, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicateStore_Put_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Put'
type ReplicateStore_Put_Call struct {
	*mock.Call
}

// Put is a helper method to define mock.On call
//  - ctx context.Context
//  - key string
//  - value api.MetaMsg
func (_e *ReplicateStore_Expecter) Put(ctx interface{}, key interface{}, value interface{}) *ReplicateStore_Put_Call {
	return &ReplicateStore_Put_Call{Call: _e.mock.On("Put", ctx, key, value)}
}

func (_c *ReplicateStore_Put_Call) Run(run func(ctx context.Context, key string, value api.MetaMsg)) *ReplicateStore_Put_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(api.MetaMsg))
	})
	return _c
}

func (_c *ReplicateStore_Put_Call) Return(_a0 error) *ReplicateStore_Put_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ReplicateStore_Put_Call) RunAndReturn(run func(context.Context, string, api.MetaMsg) error) *ReplicateStore_Put_Call {
	_c.Call.Return(run)
	return _c
}

// Remove provides a mock function with given fields: ctx, key
func (_m *ReplicateStore) Remove(ctx context.Context, key string) error {
	ret := _m.Called(ctx, key)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReplicateStore_Remove_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Remove'
type ReplicateStore_Remove_Call struct {
	*mock.Call
}

// Remove is a helper method to define mock.On call
//  - ctx context.Context
//  - key string
func (_e *ReplicateStore_Expecter) Remove(ctx interface{}, key interface{}) *ReplicateStore_Remove_Call {
	return &ReplicateStore_Remove_Call{Call: _e.mock.On("Remove", ctx, key)}
}

func (_c *ReplicateStore_Remove_Call) Run(run func(ctx context.Context, key string)) *ReplicateStore_Remove_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *ReplicateStore_Remove_Call) Return(_a0 error) *ReplicateStore_Remove_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ReplicateStore_Remove_Call) RunAndReturn(run func(context.Context, string) error) *ReplicateStore_Remove_Call {
	_c.Call.Return(run)
	return _c
}

// NewReplicateStore creates a new instance of ReplicateStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReplicateStore(t interface {
	mock.TestingT
	Cleanup(func())
}) *ReplicateStore {
	mock := &ReplicateStore{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}