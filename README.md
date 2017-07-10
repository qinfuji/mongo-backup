#### 该项目，只负责将sharding 或 replicaSet 数据 备份到 replicaSet 下

     Restore.js
        --mode  恢复方式 
          inc 增量  
          full 全量
        --uri 备份文件需要恢复到的数据库地址，当前只支持replicaSet部署
        --db  需要恢复的数据库
        --backdir 需要恢复的数据文件的位置，也是backup.js文件的输出目录

     backup.js
        --deploy 备份数据库的部署方式
          sharding 集群模式
          replSet  复制集模式
        --mode   备份方式
          inc    增量备份
          full   全量备份
        --backupdir 备份输出目录
        --uri    待备份的数据库地址
        --db     待备份的数据库

     
     备份输出目录结构

        backupdir
           |-- dbname   
              |--full  全量目录
              |-—inc   增量目录
              |--incfinish 增量完成后后将文件拷贝到该目录下，防止恢复与备份文件冲突  
              |--*_status.json  备份的状态记录，每次备份(增量、全量)都需要修，增量备份使用该文件。
                


#### 首先得到sharding集群或replcaSet的Hide node或SECONDARY node
#### 如果是sharding则取消 banlance 能力， 防止 chunk 迁移
#### 如果是sharding 则需要找到关键shard,关键shard的数据比非关键的多，在restor时需要先装载非关键shard数据，并补不重建索引，在restore关键shard时启用重建索引
#### 枷锁 hide node 或 SECONDARY node  fsyncLock , 停止在备份期间写入oplog
#### 记录各个节点的 oplog时间点，为后续的增量备份做准备
#### 使用mongodump 对数据进行备份
#### 进行增量备份，直接读取local结合中的数据？ 全量备份后是否需要处理？ local.oplog.rs是否能够直接取对指定数据库的操作？
#### 解锁 fsyncUnlock , 开始同步数据log
