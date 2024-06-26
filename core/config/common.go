/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import "sync"

type Option[T any] interface {
	Apply(object T)
}

type OptionFunc[T any] func(object T)

func (o OptionFunc[T]) Apply(object T) {
	o(object)
}

type RetrySettings struct {
	RetryTimes  int
	InitBackOff int
	MaxBackOff  int
}

type EtcdRetryConfig struct {
	Retry RetrySettings
}

var (
	configInstance   *CommonConfig
	commonConfigOnce sync.Once
)

type ConfigOption func(config *CommonConfig)

func InitCommonConfig(options ...ConfigOption) {
	commonConfigOnce.Do(func() {
		configInstance = &CommonConfig{}
		for _, option := range options {
			option(configInstance)
		}
	})
}

func GetCommonConfig() *CommonConfig {
	if configInstance == nil {
		InitCommonConfig()
	}
	return configInstance
}

type CommonConfig struct {
	Retry RetrySettings
}
