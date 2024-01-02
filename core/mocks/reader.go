// Code generated by mockery v2.32.4. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Reader is an autogenerated mock type for the Reader type
type Reader struct {
	mock.Mock
}

type Reader_Expecter struct {
	mock *mock.Mock
}

func (_m *Reader) EXPECT() *Reader_Expecter {
	return &Reader_Expecter{mock: &_m.Mock}
}

// ErrorChan provides a mock function with given fields:
func (_m *Reader) ErrorChan() <-chan error {
	ret := _m.Called()

	var r0 <-chan error
	if rf, ok := ret.Get(0).(func() <-chan error); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan error)
		}
	}

	return r0
}

// Reader_ErrorChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ErrorChan'
type Reader_ErrorChan_Call struct {
	*mock.Call
}

// ErrorChan is a helper method to define mock.On call
func (_e *Reader_Expecter) ErrorChan() *Reader_ErrorChan_Call {
	return &Reader_ErrorChan_Call{Call: _e.mock.On("ErrorChan")}
}

func (_c *Reader_ErrorChan_Call) Run(run func()) *Reader_ErrorChan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *Reader_ErrorChan_Call) Return(_a0 <-chan error) *Reader_ErrorChan_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Reader_ErrorChan_Call) RunAndReturn(run func() <-chan error) *Reader_ErrorChan_Call {
	_c.Call.Return(run)
	return _c
}

// QuitRead provides a mock function with given fields: ctx
func (_m *Reader) QuitRead(ctx context.Context) {
	_m.Called(ctx)
}

// Reader_QuitRead_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'QuitRead'
type Reader_QuitRead_Call struct {
	*mock.Call
}

// QuitRead is a helper method to define mock.On call
//  - ctx context.Context
func (_e *Reader_Expecter) QuitRead(ctx interface{}) *Reader_QuitRead_Call {
	return &Reader_QuitRead_Call{Call: _e.mock.On("QuitRead", ctx)}
}

func (_c *Reader_QuitRead_Call) Run(run func(ctx context.Context)) *Reader_QuitRead_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *Reader_QuitRead_Call) Return() *Reader_QuitRead_Call {
	_c.Call.Return()
	return _c
}

func (_c *Reader_QuitRead_Call) RunAndReturn(run func(context.Context)) *Reader_QuitRead_Call {
	_c.Call.Return(run)
	return _c
}

// StartRead provides a mock function with given fields: ctx
func (_m *Reader) StartRead(ctx context.Context) {
	_m.Called(ctx)
}

// Reader_StartRead_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartRead'
type Reader_StartRead_Call struct {
	*mock.Call
}

// StartRead is a helper method to define mock.On call
//  - ctx context.Context
func (_e *Reader_Expecter) StartRead(ctx interface{}) *Reader_StartRead_Call {
	return &Reader_StartRead_Call{Call: _e.mock.On("StartRead", ctx)}
}

func (_c *Reader_StartRead_Call) Run(run func(ctx context.Context)) *Reader_StartRead_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *Reader_StartRead_Call) Return() *Reader_StartRead_Call {
	_c.Call.Return()
	return _c
}

func (_c *Reader_StartRead_Call) RunAndReturn(run func(context.Context)) *Reader_StartRead_Call {
	_c.Call.Return(run)
	return _c
}

// NewReader creates a new instance of Reader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *Reader {
	mock := &Reader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}