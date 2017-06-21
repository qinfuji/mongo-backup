该项目，只负责将sharding 或 replicaSet 数据 备份到 replicaSet 下

如果系统有安全认证，为简单起见，对复制集、 mongos 使用统一的用户进行备份


#### 首先得到sharding集群或replcaSet的Hide node或SECONDARY node
#### 如果是sharding则取消 banlance 能力， 防止 chunk 迁移
#### 如果是sharding 则需要找到关键shard,关键shard的数据比非关键的多，在restor时需要先装载非关键shard数据，并补不重建索引，在restore关键shard时启用重建索引
#### 枷锁 hide node 或 SECONDARY node  fsyncLock , 停止在备份期间写入oplog
#### 记录各个节点的 oplog时间点，为后续的增量备份做准备
#### 使用mongodump 对数据进行备份
#### 进行增量备份，直接读取local结合中的数据？ 全量备份后是否需要处理？ local.oplog.rs是否能够直接取对指定数据库的操作？
#### 解锁 fsyncUnlock , 开始同步数据log
