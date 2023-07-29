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

package server

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-cdc/server/metrics"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/core/writer"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"go.uber.org/zap"
)

var EmptyCdcTask = &CDCTask{}

type signal struct {
	state meta.TaskState
	done  chan error
	f     func() error
}

type CDCTask struct {
	id            string
	factory       CDCFactory
	callback      writer.WriteCallback
	writeFailFunc func() error
	signaler      chan *signal
	current       util.Value[meta.TaskState]
	workingLock   sync.Mutex
}

func NewCdcTask(taskID string, f CDCFactory, c writer.WriteCallback, w func() error) *CDCTask {
	task := &CDCTask{
		id:            taskID,
		factory:       f,
		callback:      c,
		writeFailFunc: w,
		signaler:      make(chan *signal),
	}
	task.current.Store(meta.TaskStateInitial)
	go task.handle()
	return task
}

func (c *CDCTask) Pause(p func() error) <-chan error {
	d := make(chan error, 1)
	c.sendSignal(&signal{meta.TaskStatePaused, d, p})
	return d
}

func (c *CDCTask) Resume(r func() error) <-chan error {
	d := make(chan error, 1)
	c.sendSignal(&signal{meta.TaskStateRunning, d, r})
	return d
}

func (c *CDCTask) Terminate(t func() error) <-chan error {
	d := make(chan error, 1)
	c.sendSignal(&signal{meta.TaskStateTerminate, d, t})
	return d
}

func (c *CDCTask) handle() {
	done := make(chan struct{})
	for {
		s := <-c.signaler
		executeF := func() error {
			if s.f == nil {
				return nil
			}
			return s.f()
		}

		if s.state == c.current.Load() {
			c.handleDone(s.done, nil)
			continue
		}
		if err := c.stateCheck(s.state); err != nil {
			c.handleDone(s.done, err)
			continue
		}

		switch s.state {
		case meta.TaskStateRunning:
			cdcReader, err := c.factory.NewReader()
			if err != nil {
				c.handleDone(s.done, errors.WithMessage(err, "fail to create reader"))
				continue
			}
			cdcWriter, err := c.factory.NewWriter()
			if err != nil {
				c.handleDone(s.done, errors.WithMessage(err, "fail to create writer"))
				continue
			}
			if err = executeF(); err != nil {
				c.handleDone(s.done, errors.WithMessagef(err, "fail to change the task state, from %s to %s",
					c.current.Load().String(), meta.TaskStateRunning.String()))
				continue
			}
			go c.work(done, cdcReader, cdcWriter)
			c.current.Store(meta.TaskStateRunning)
			c.handleDone(s.done, nil)
		case meta.TaskStatePaused:
			if err := executeF(); err != nil {
				c.handleDone(s.done, errors.WithMessagef(err, "fail to change the task state, from %s to %s",
					c.current.Load().String(), meta.TaskStatePaused.String()))
				continue
			}
			c.current.Store(meta.TaskStatePaused)
			done <- struct{}{}
			c.handleDone(s.done, nil)
		case meta.TaskStateTerminate:
			if err := executeF(); err != nil {
				c.handleDone(s.done, errors.WithMessagef(err, "fail to change the task state, from %s to %s",
					c.current.Load().String(), meta.TaskStateTerminate.String()))
				continue
			}
			if c.current.Load() == meta.TaskStateRunning {
				done <- struct{}{}
			}
			c.current.Store(meta.TaskStateTerminate)
			c.handleDone(s.done, nil)
			return
		default:
			log.Warn("unknown signal", zap.String("signal", s.state.String()))
		}
	}
}

func (c *CDCTask) work(done <-chan struct{}, cdcReader reader.CDCReader, cdcWriter writer.CDCWriter) {
	c.workingLock.Lock()
	defer c.workingLock.Unlock()

	dataChan := cdcReader.StartRead(context.Background())
	writeData := func(data *model.CDCData) {
		var msgType string
		var count int
		var collectionID int64
		if data.Msg.Type() == commonpb.MsgType_Insert {
			msg := data.Msg.(*msgstream.InsertMsg)
			msgType = commonpb.MsgType_Insert.String()
			count = len(msg.RowIDs)
			collectionID = msg.CollectionID
		} else if data.Msg.Type() == commonpb.MsgType_Delete {
			msg := data.Msg.(*msgstream.DeleteMsg)
			msgType = commonpb.MsgType_Delete.String()
			count = int(msg.NumRows)
			collectionID = msg.CollectionID
		}
		if msgType != "" {
			metrics.ReadMsgRowCountVec.WithLabelValues(c.id, strconv.FormatInt(collectionID, 10), msgType).Add(float64(count))
		}

		if err := cdcWriter.Write(context.Background(), data, c.callback); err != nil {
			log.Warn("fail to write the data", zap.Any("data", data), zap.Error(err))
			err = <-c.Pause(c.writeFailFunc)
			if err != nil {
				log.Warn("fail to pause inner", zap.Error(err))
			}
		}
	}
	quit := func() {
		cdcReader.QuitRead(context.Background())
		for {
			select {
			case data := <-dataChan:
				writeData(data)
			default:
				cdcWriter.Flush(context.Background())
				return
			}
		}
	}

	for {
		select {
		case <-done:
			quit()
			return
		default:
			select {
			case data := <-dataChan:
				writeData(data)
			case <-done:
				quit()
				return
			}
		}
	}
}

func (c *CDCTask) handleDone(d chan error, err error) {
	d <- err
	close(d)
}

func (c *CDCTask) stateCheck(state meta.TaskState) error {
	currentState := c.current.Load()
	if currentState == state {
		return fmt.Errorf("the current task state is similar to the target state, current state: %s", currentState.String())
	}
	if state == meta.TaskStatePaused && currentState != meta.TaskStateRunning {
		return fmt.Errorf("the task state isn't running, current state: %s", currentState.String())
	}
	if state == meta.TaskStateRunning && currentState == meta.TaskStateTerminate {
		return fmt.Errorf("the task has terminated")
	}
	return nil
}

func (c *CDCTask) sendSignal(s *signal) {
	if err := c.stateCheck(s.state); err != nil {
		log.Warn("fail to check the task state", zap.Error(err))
		c.handleDone(s.done, err)
		return
	}

	if c.current.Load() == meta.TaskStateTerminate {
		log.Warn("the task has terminated")
		c.handleDone(s.done, fmt.Errorf("the task has terminated"))
		return
	}

	c.signaler <- s
}

//go:generate mockery --name=CDCFactory --filename=cdc_factory_mock.go --output=./mocks
type CDCFactory interface {
	util.CDCMark
	NewReader() (reader.CDCReader, error)
	NewWriter() (writer.CDCWriter, error)
}

type NewReaderFunc func() (reader.CDCReader, error)
type NewWriterFunc func() (writer.CDCWriter, error)
type FactoryCreator func(readerFunc NewReaderFunc, writerFunc NewWriterFunc) CDCFactory

type DefaultCDCFactory struct {
	util.CDCMark
	newReader NewReaderFunc
	newWriter NewWriterFunc
}

func NewDefaultCDCFactory(r NewReaderFunc, w NewWriterFunc) CDCFactory {
	return &DefaultCDCFactory{newReader: r, newWriter: w}
}

func (d *DefaultCDCFactory) NewReader() (reader.CDCReader, error) {
	return d.newReader()
}

func (d *DefaultCDCFactory) NewWriter() (writer.CDCWriter, error) {
	return d.newWriter()
}

func NewCDCFactory(readerFunc NewReaderFunc, writerFunc NewWriterFunc) CDCFactory {
	return NewDefaultCDCFactory(readerFunc, writerFunc)
}
