package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

type MetaConfig struct {
	EtcdAddress        []string
	TaskPositionPrefix string
	TaskPositionKey    string
	Timeout            int
}

func main() {
	paramtable.Init()
	log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{
		Core:   zapcore.NewNopCore(),
		Syncer: zapcore.AddSync(ioutil.Discard),
		Level:  zap.NewAtomicLevel(),
	})
	server.PrintInfo()

	fileContent, err := os.ReadFile("./configs/meta.yaml")
	if err != nil {
		panic(err)
	}
	var metaConfig MetaConfig
	err = yaml.Unmarshal(fileContent, &metaConfig)
	if err != nil {
		panic(err)
	}

	client, err := clientv3.New(clientv3.Config{Endpoints: metaConfig.EtcdAddress})
	if err != nil {
		panic(err)
	}

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(metaConfig.Timeout)*time.Second)
	defer cancelFunc()
	var getResp *clientv3.GetResponse
	if metaConfig.TaskPositionKey != "" {
		getResp, err = client.Get(timeoutCtx, fmt.Sprintf("%s/%s", metaConfig.TaskPositionPrefix, metaConfig.TaskPositionKey))
	} else {
		getResp, err = client.Get(timeoutCtx, metaConfig.TaskPositionPrefix, clientv3.WithPrefix())
	}
	if err != nil {
		panic(err)
	}
	if len(getResp.Kvs) == 0 {
		panic("task position not exist")
	}
	for _, kv := range getResp.Kvs {
		taskPosition := &meta.TaskCollectionPosition{}
		err := json.Unmarshal(kv.Value, taskPosition)
		if err != nil {
			panic(err)
		}
		fmt.Println("task id:", taskPosition.TaskID)
		fmt.Println("collection id:", taskPosition.CollectionID)
		fmt.Println("collection name:", taskPosition.CollectionName)
		fmt.Println("-------positions-------")
		for s, pair := range taskPosition.Positions {
			fmt.Println(fmt.Sprintf("%s: %s", s, util.Base64MsgPosition(&msgstream.MsgPosition{
				ChannelName: s,
				MsgID:       pair.Data,
			})))
		}
		fmt.Println("+++++++++++++++++++++++++++")
	}
}
