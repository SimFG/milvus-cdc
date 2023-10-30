// Code generated by mockery v2.32.4. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	config "github.com/zilliztech/milvus-cdc/core/config"

	msgstream "github.com/milvus-io/milvus/pkg/mq/msgstream"
)

// FactoryCreator is an autogenerated mock type for the FactoryCreator type
type FactoryCreator struct {
	mock.Mock
}

type FactoryCreator_Expecter struct {
	mock *mock.Mock
}

func (_m *FactoryCreator) EXPECT() *FactoryCreator_Expecter {
	return &FactoryCreator_Expecter{mock: &_m.Mock}
}

// NewKmsFactory provides a mock function with given fields: cfg
func (_m *FactoryCreator) NewKmsFactory(cfg *config.KafkaConfig) msgstream.Factory {
	ret := _m.Called(cfg)

	var r0 msgstream.Factory
	if rf, ok := ret.Get(0).(func(*config.KafkaConfig) msgstream.Factory); ok {
		r0 = rf(cfg)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(msgstream.Factory)
		}
	}

	return r0
}

// FactoryCreator_NewKmsFactory_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'NewKmsFactory'
type FactoryCreator_NewKmsFactory_Call struct {
	*mock.Call
}

// NewKmsFactory is a helper method to define mock.On call
//   - cfg *config.KafkaConfig
func (_e *FactoryCreator_Expecter) NewKmsFactory(cfg interface{}) *FactoryCreator_NewKmsFactory_Call {
	return &FactoryCreator_NewKmsFactory_Call{Call: _e.mock.On("NewKmsFactory", cfg)}
}

func (_c *FactoryCreator_NewKmsFactory_Call) Run(run func(cfg *config.KafkaConfig)) *FactoryCreator_NewKmsFactory_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*config.KafkaConfig))
	})
	return _c
}

func (_c *FactoryCreator_NewKmsFactory_Call) Return(_a0 msgstream.Factory) *FactoryCreator_NewKmsFactory_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *FactoryCreator_NewKmsFactory_Call) RunAndReturn(run func(*config.KafkaConfig) msgstream.Factory) *FactoryCreator_NewKmsFactory_Call {
	_c.Call.Return(run)
	return _c
}

// NewPmsFactory provides a mock function with given fields: cfg
func (_m *FactoryCreator) NewPmsFactory(cfg *config.PulsarConfig) msgstream.Factory {
	ret := _m.Called(cfg)

	var r0 msgstream.Factory
	if rf, ok := ret.Get(0).(func(*config.PulsarConfig) msgstream.Factory); ok {
		r0 = rf(cfg)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(msgstream.Factory)
		}
	}

	return r0
}

// FactoryCreator_NewPmsFactory_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'NewPmsFactory'
type FactoryCreator_NewPmsFactory_Call struct {
	*mock.Call
}

// NewPmsFactory is a helper method to define mock.On call
//   - cfg *config.PulsarConfig
func (_e *FactoryCreator_Expecter) NewPmsFactory(cfg interface{}) *FactoryCreator_NewPmsFactory_Call {
	return &FactoryCreator_NewPmsFactory_Call{Call: _e.mock.On("NewPmsFactory", cfg)}
}

func (_c *FactoryCreator_NewPmsFactory_Call) Run(run func(cfg *config.PulsarConfig)) *FactoryCreator_NewPmsFactory_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*config.PulsarConfig))
	})
	return _c
}

func (_c *FactoryCreator_NewPmsFactory_Call) Return(_a0 msgstream.Factory) *FactoryCreator_NewPmsFactory_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *FactoryCreator_NewPmsFactory_Call) RunAndReturn(run func(*config.PulsarConfig) msgstream.Factory) *FactoryCreator_NewPmsFactory_Call {
	_c.Call.Return(run)
	return _c
}

// NewFactoryCreator creates a new instance of FactoryCreator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewFactoryCreator(t interface {
	mock.TestingT
	Cleanup(func())
}) *FactoryCreator {
	mock := &FactoryCreator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}