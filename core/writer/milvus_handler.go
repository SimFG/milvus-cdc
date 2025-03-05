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

package writer

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type MilvusDataHandler struct {
	api.DataHandler

	uri             string
	token           string
	enableTLS       bool
	ignorePartition bool // sometimes the has partition api is a deny api
	connectTimeout  int
	retryOptions    []retry.Option
	dialConfig      util.DialConfig
}

func NewMilvusDataHandler(options ...config.Option[*MilvusDataHandler]) (*MilvusDataHandler, error) {
	handler := &MilvusDataHandler{
		connectTimeout: 5,
	}
	for _, option := range options {
		option.Apply(handler)
	}
	if handler.uri == "" {
		return nil, errors.New("empty milvus connect uri")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	err = handler.milvusOp(timeoutContext, "", func(milvus *milvusclient.Client) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the milvus client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)
	return handler, nil
}

func (m *MilvusDataHandler) milvusOp(ctx context.Context, database string, f func(milvus *milvusclient.Client) error) error {
	retryMilvusFunc := func() error {
		// TODO Retryable and non-retryable errors should be distinguished
		var err error
		var c *milvusclient.Client
		retryErr := retry.Do(ctx, func() error {
			c, err = util.GetMilvusClientManager().GetMilvusClient(ctx, m.uri, m.token, database, m.dialConfig)
			if err != nil {
				log.Warn("fail to get milvus client", zap.Error(err))
				return err
			}
			err = f(c)
			if status.Code(err) == codes.Canceled {
				util.GetMilvusClientManager().DeleteMilvusClient(m.uri, database)
				log.Warn("grpc: the client connection is closing, waiting...", zap.Error(err))
				time.Sleep(resource.DefaultExpiration)
			}
			return err
		}, m.retryOptions...)
		if retryErr != nil && err != nil {
			return err
		}
		if retryErr != nil {
			return retryErr
		}
		return nil
	}

	return retryMilvusFunc()
}

func (m *MilvusDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	createCollectionOption := milvusclient.NewCreateCollectionOption(param.Schema.CollectionName, param.Schema)
	describeCollectionOption := milvusclient.NewDescribeCollectionOption(param.Schema.CollectionName)

	for _, property := range param.Properties {
		createCollectionOption.WithProperty(property.GetKey(), property.GetValue())
	}
	createCollectionOption.WithShardNum(param.ShardsNum)
	createCollectionOption.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel))
	// TODO fubang Should Add the BASE
	// createCollectionOption.WithCreateCollectionMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		if _, err := milvus.DescribeCollection(ctx, describeCollectionOption); err == nil {
			log.Info("skip to create collection, because it's has existed", zap.String("collection", param.Schema.CollectionName))
			return nil
		}
		return milvus.CreateCollection(ctx, createCollectionOption)
	})
}

func (m *MilvusDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	dropCollectionOption := milvusclient.NewDropCollectionOption(param.CollectionName)
	// TODO fubang Should Add the BASE
	// dropCollectionOption.WithDropCollectionMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		return milvus.DropCollection(ctx, dropCollectionOption)
	})
}

// TODO no used
func (m *MilvusDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in insert request", zap.String("partition", partitionName))
		partitionName = ""
	}
	insertOption := milvusclient.NewColumnBasedInsertOption(param.CollectionName, param.Columns...)
	insertOption.WithPartition(partitionName)
	// TODO Should Add the BASE
	// insertOption.WithInsertMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.Insert(ctx, insertOption)
		return err
	})
}

// TODO no used
func (m *MilvusDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in delete request", zap.String("partition", partitionName))
		partitionName = ""
	}
	deleteOption := milvusclient.NewDeleteOption(param.CollectionName)
	deleteOption.WithPartition(partitionName)
	pkName := param.Column.Name()
	// TODO Should Add the BASE
	if data := param.Column.FieldData().GetScalars().GetLongData().GetData(); data != nil {
		deleteOption.WithInt64IDs(pkName, data)
	} else if data := param.Column.FieldData().GetScalars().GetStringData().GetData(); data != nil {
		deleteOption.WithStringIDs(pkName, data)
	} else {
		return errors.New("unsupported data type")
	}

	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.Delete(ctx, deleteOption)
		return err
	})
}

func (m *MilvusDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore create partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	createPartitionParam := milvusclient.NewCreatePartitionOption(param.CollectionName, param.PartitionName)
	// TODO fubang Should Add the BASE
	// createPartitionParam.WithCreatePartitionMsgBase(param.Base)
	listPartitionParam := milvusclient.NewListPartitionOption(param.CollectionName)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		partitions, err := milvus.ListPartitions(ctx, listPartitionParam)
		if err != nil {
			log.Warn("fail to show partitions", zap.String("collection", param.CollectionName), zap.Error(err))
			return err
		}
		for _, partitionName := range partitions {
			if partitionName == param.PartitionName {
				log.Info("skip to create partition, because it's has existed",
					zap.String("collection", param.CollectionName),
					zap.String("partition", param.PartitionName))
				return nil
			}
		}
		return milvus.CreatePartition(ctx, createPartitionParam)
	})
}

func (m *MilvusDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore drop partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	dropPartitionParam := milvusclient.NewDropPartitionOption(param.CollectionName, param.PartitionName)
	// TODO fubang Should Add the BASE
	// dropPartitionParam.WithDropPartitionMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		return milvus.DropPartition(ctx, dropPartitionParam)
	})
}

func (m *MilvusDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	// TODO fubang set index type
	indexParam := index.NewGenericIndex(param.GetIndexName(), util.ConvertKVPairToMap(param.GetExtraParams()))
	createIndexParam := milvusclient.NewCreateIndexOption(param.CollectionName, param.FieldName, indexParam)
	// TODO fubang Should Add the BASE
	// createIndexParam.WithIndexMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		indexTask, err := milvus.CreateIndex(ctx, createIndexParam)
		if err != nil {
			return err
		}
		return indexTask.Await(ctx)
	})
}

func (m *MilvusDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	dropIndexParam := milvusclient.NewDropIndexOption(param.CollectionName, param.IndexName)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		return milvus.DropIndex(ctx, dropIndexParam)
	})
}

func (m *MilvusDataHandler) AlterIndex(ctx context.Context, param *api.AlterIndexParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		// extraParams := util.ConvertKVPairToMap(param.GetExtraParams())
		// mmapEnable, _ := strconv.ParseBool(extraParams[api.IndexKeyMmap])
		// return milvus.AlterIndex(ctx, param.CollectionName, param.IndexName,
		// 	milvusclient.WithMmap(mmapEnable),
		// 	milvusclient.WithIndexMsgBase(param.GetBase()),
		// )
		// TODO fubang alter index
		return nil
	})
}

func (m *MilvusDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	// TODO resource group
	loadCollectionParam := milvusclient.NewLoadCollectionOption(param.CollectionName)
	loadCollectionParam.WithReplica(int(param.GetReplicaNumber()))
	loadCollectionParam.WithRefresh(param.GetRefresh())
	loadCollectionParam.WithLoadFields(param.GetLoadFields()...)
	loadCollectionParam.WithSkipLoadDynamicField(param.GetSkipLoadDynamicField())
	// TODO fubang Should Add the BASE
	// loadCollectionParam.WithLoadCollectionMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		task, err := milvus.LoadCollection(ctx, loadCollectionParam)
		if err != nil {
			return err
		}
		return task.Await(ctx)
	})
}

func (m *MilvusDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	releaseCollectionParam := milvusclient.NewReleaseCollectionOption(param.CollectionName)
	// TODO fubang Should Add the BASE
	// releaseCollectionParam.WithReleaseCollectionMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		return milvus.ReleaseCollection(ctx, releaseCollectionParam)
	})
}

func (m *MilvusDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	loadPartitionsParam := milvusclient.NewLoadPartitionsOption(param.CollectionName, param.PartitionNames...)
	// TODO fubang Should Add the BASE
	// loadPartitionsParam.WithLoadPartitionsMsgBase(param.GetBase())
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		task, err := milvus.LoadPartitions(ctx, loadPartitionsParam)
		if err != nil {
			return err
		}
		return task.Await(ctx)
	})
}

func (m *MilvusDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	releasePartitonsParam := milvusclient.NewReleasePartitionsOptions(param.CollectionName, param.PartitionNames...)
	// TODO fubang Should Add the BASE
	// releasePartitonsParam.WithReleasePartitionMsgBase(param.GetBase())
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		return milvus.ReleasePartitions(ctx, releasePartitonsParam)
	})
}

func (m *MilvusDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	for _, s := range param.GetCollectionNames() {
		flushParam := milvusclient.NewFlushOption(s)
		// TODO fubang Should Add the BASE
		// flushParam.WithFlushMsgBase(param.GetBase())
		if err := m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
			flushTask, err := milvus.Flush(ctx, flushParam)
			if err != nil {
				return err
			}
			return flushTask.Await(ctx)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *MilvusDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	listDatabaseParam := milvusclient.NewListDatabaseOption()
	createDatabaseParam := milvusclient.NewCreateDatabaseOption(param.DbName)
	// TODO fubang Should Add the BASE
	// createDatabaseParam.WithCreateDatabaseMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		databases, err := milvus.ListDatabase(ctx, listDatabaseParam)
		if err != nil {
			return err
		}
		for _, databaseName := range databases {
			if databaseName == param.DbName {
				log.Info("skip to create database, because it's has existed", zap.String("database", param.DbName))
				return nil
			}
		}

		return milvus.CreateDatabase(ctx, createDatabaseParam)
	})
}

func (m *MilvusDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	dropDatabaseParam := milvusclient.NewDropDatabaseOption(param.DbName)
	// TODO fubang Should Add the BASE
	// dropDatabaseParam.WithDropDatabaseMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.DropDatabase(ctx, dropDatabaseParam)
	})
}

func (m *MilvusDataHandler) AlterDatabase(ctx context.Context, param *api.AlterDatabaseParam) error {
	if len(param.GetProperties()) == 0 {
		// TODO fubang should add the delete keys
		alterDatabasePropertiesParam := milvusclient.NewAlterDatabasePropertiesOption(param.DbName)
		for _, p := range param.GetProperties() {
			alterDatabasePropertiesParam.WithProperty(p.GetKey(), p.GetValue())
		}
		// TODO fubang Should Add the BASE
		// alterDatabasePropertiesParam.WithAlterDatabasePropertiesMsgBase(param.GetBase())
		return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
			return milvus.AlterDatabaseProperties(ctx, alterDatabasePropertiesParam)
		})
	}
	if len(param.GetDeleteKeys()) > 0 {
		// TODO fubang should add the delete keys
		dropDatabasePropertiesParam := milvusclient.NewDropDatabasePropertiesOption(param.DbName, param.GetDeleteKeys()...)
		// TODO fubang Should Add the BASE
		// alterDatabasePropertiesParam.WithAlterDatabasePropertiesMsgBase(param.GetBase())
		return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
			return milvus.DropDatabaseProperties(ctx, dropDatabasePropertiesParam)
		})
	}
	return nil
}

func (m *MilvusDataHandler) CreateUser(ctx context.Context, param *api.CreateUserParam) error {
	pwd, err := DecodePwd(param.Password)
	if err != nil {
		return err
	}
	createUserParam := milvusclient.NewCreateUserOption(param.Username, pwd)
	// TODO fubang Should Add the BASE
	// createUserParam.WithCreateUserMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.CreateUser(ctx, createUserParam)
	})
}

func (m *MilvusDataHandler) DeleteUser(ctx context.Context, param *api.DeleteUserParam) error {
	dropUserParam := milvusclient.NewDropUserOption(param.Username)
	// TODO fubang Should Add the BASE
	// dropUserParam.WithDropUserMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.DropUser(ctx, dropUserParam)
	})
}

func (m *MilvusDataHandler) UpdateUser(ctx context.Context, param *api.UpdateUserParam) error {
	oldPwd, err := DecodePwd(param.OldPassword)
	if err != nil {
		return err
	}
	newPwd, err := DecodePwd(param.NewPassword)
	if err != nil {
		return err
	}
	updatePasswordParam := milvusclient.NewUpdatePasswordOption(param.Username, oldPwd, newPwd)
	// TODO fubang Should Add the BASE
	// updatePasswordParam.WithUpdatePasswordMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.UpdatePassword(ctx, updatePasswordParam)
	})
}

func DecodePwd(pwd string) (string, error) {
	return crypto.Base64Decode(pwd)
}

func (m *MilvusDataHandler) CreateRole(ctx context.Context, param *api.CreateRoleParam) error {
	createRoleParam := milvusclient.NewCreateRoleOption(param.GetEntity().GetName())
	// TODO fubang Should Add the BASE
	// createRoleParam.WithCreateRoleMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.CreateRole(ctx, createRoleParam)
	})
}

func (m *MilvusDataHandler) DropRole(ctx context.Context, param *api.DropRoleParam) error {
	dropRoleParam := milvusclient.NewDropRoleOption(param.GetRoleName())
	// TODO fubang Should Add the BASE
	// dropRoleParam.WithDropRoleMsgBase(param.GetBase())
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		return milvus.DropRole(ctx, dropRoleParam)
	})
}

func (m *MilvusDataHandler) OperateUserRole(ctx context.Context, param *api.OperateUserRoleParam) error {
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		switch param.Type {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			grantRoleParam := milvusclient.NewGrantRoleOption(param.Username, param.RoleName)
			// TODO fubang Should Add the BASE
			// grantRoleParam.WithOperateUserRoleMsgBase(param.GetBase())
			return milvus.GrantRole(ctx, grantRoleParam)
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			revokeRoleParam := milvusclient.NewRevokeRoleOption(param.Username, param.RoleName)
			// TODO fubang Should Add the BASE
			// revokeRoleParam.WithOperateUserRoleMsgBase(param.GetBase())
			return milvus.RevokeRole(ctx, revokeRoleParam)
		default:
			log.Warn("unknown operate user role type", zap.String("type", param.Type.String()))
			return nil
		}
	})
}

func (m *MilvusDataHandler) OperatePrivilege(ctx context.Context, param *api.OperatePrivilegeParam) error {
	objectType := param.GetEntity().GetObject().GetName()

	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		switch param.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			grantParam := milvusclient.NewGrantPrivilegeOption(
				param.GetEntity().GetRole().GetName(),
				objectType,
				param.GetEntity().GetObjectName(),
				param.GetEntity().GetGrantor().GetPrivilege().GetName(),
			)
			// TODO fubang Should Add the BASE
			// grantParam.WithOperatePrivilegeMsgBase(param.GetBase())
			return milvus.GrantPrivilege(ctx, grantParam)
		case milvuspb.OperatePrivilegeType_Revoke:
			revokeParam := milvusclient.NewRevokePrivilegeOption(
				param.GetEntity().GetRole().GetName(),
				objectType,
				param.GetEntity().GetObjectName(),
				param.GetEntity().GetGrantor().GetPrivilege().GetName(),
			)
			// TODO fubang Should Add the BASE
			// revokeParam.WithOperatePrivilegeMsgBase(param.GetBase())
			return milvus.RevokePrivilege(ctx, revokeParam)
		default:
			log.Warn("unknown operate privilege type", zap.String("type", param.Type.String()))
			return nil
		}
	})
}

func (m *MilvusDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	// TODO fubang
	// var (
	// 	resp  *entity.MessageInfo
	// 	err   error
	// 	opErr error
	// )
	// opErr = m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
	// 	resp, err = milvus.ReplicateMessage(ctx, param.ChannelName,
	// 		param.BeginTs, param.EndTs,
	// 		param.MsgsBytes,
	// 		param.StartPositions, param.EndPositions,
	// 		milvusclient.WithReplicateMessageMsgBase(param.Base))
	// 	return err
	// })
	// if err != nil {
	// 	return err
	// }
	// if opErr != nil {
	// 	return opErr
	// }
	// param.TargetMsgPosition = resp.Position
	// return nil
	return nil
}

func (m *MilvusDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	describeCollectionParam := milvusclient.NewDescribeCollectionOption(param.Name)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.DescribeCollection(ctx, describeCollectionParam)
		return err
	})
}

func (m *MilvusDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	listDatabaseParam := milvusclient.NewListDatabaseOption()
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		databases, err := milvus.ListDatabase(ctx, listDatabaseParam)
		if err != nil {
			return err
		}
		for _, databaseName := range databases {
			if databaseName == param.Name {
				return nil
			}
		}
		return errors.Newf("database [%s] not found", param.Name)
	})
}

func (m *MilvusDataHandler) DescribePartition(ctx context.Context, param *api.DescribePartitionParam) error {
	listPartitionsParam := milvusclient.NewListPartitionOption(param.CollectionName)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		partitions, err := milvus.ListPartitions(ctx, listPartitionsParam)
		if err != nil {
			return err
		}
		for _, partitionName := range partitions {
			if partitionName == param.PartitionName {
				return nil
			}
		}
		return errors.Newf("partition [%s] not found", param.PartitionName)
	})
}
