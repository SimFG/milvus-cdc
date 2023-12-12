// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"time"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server"
)

func main() {
	paramtable.Init()
	log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{
		Core:   zapcore.NewNopCore(),
		Syncer: zapcore.AddSync(ioutil.Discard),
		Level:  zap.NewAtomicLevel(),
	})

	server.LogInfo()

	s := &server.CDCServer{}

	// parse config file
	fileContent, _ := os.ReadFile("./configs/cdc.yaml")
	var serverConfig server.CDCServerConfig
	err := yaml.Unmarshal(fileContent, &serverConfig)
	if err != nil {
		util.Log.Panic("Failed to parse config file", zap.Error(err))
	}
	if serverConfig.Pprof {
		go collectionPPROF(serverConfig.PprofInterval)
	}
	s.Run(&serverConfig)
}

func collectionPPROF(interval int) {
	t := time.Duration(interval) * time.Second
	ticker := time.NewTicker(t)
	defer ticker.Stop()

	for range ticker.C {
		allocsFile, err := os.Create(fmt.Sprintf("pprof_allocs_%s.profile", time.Now().String()))
		if err != nil {
			util.Log.Info("fail to create data")
			return
		}
		pprof.Lookup("allocs").WriteTo(allocsFile, 0)
		allocsFile.Close()

		heapFile, err := os.Create(fmt.Sprintf("pprof_heap_%s.profile", time.Now().String()))
		if err != nil {
			util.Log.Info("fail to create data")
			return
		}
		pprof.Lookup("heap").WriteTo(heapFile, 0)
		heapFile.Close()

		util.Log.Info("success to collect pprof data")
	}
}
