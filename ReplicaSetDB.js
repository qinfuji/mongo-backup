var Node = require("./Node")
const DB = require("./DB")
var fs = require('fs');
var Result = require("./Result")
const Timestamp = require('mongodb').Timestamp;

function ReplicaSetDB(url) {
    DB.apply(this, [url]);
}

ReplicaSetDB.prototype.constructor = ReplicaSetDB;
ReplicaSetDB.prototype = Object.create(DB.prototype);


ReplicaSetDB.prototype.fullbackup = async function(backupInfo) {

    let db = await this.getDb();
    let secondaryNode = await this.getSecondaryNode()
    console.log(`ReplicaSetDB fullbackup ${this.url} : ${secondaryNode.toString()}`)
    try {
        let startTime = new Date().getTime();
        let uriInfo = this.getUriInfo();
        //let lockRet = await secondaryNode.fsyncLock(); //加入锁
        let oplogTime = await secondaryNode.oplogTimestamp() //得到最后的oplog时间
        let backupDir = backupInfo.backup_dir + "/full" + "/" + uriInfo.replSetName; //全量备份目录
        console.log(`start backup  ReplicaSetDB ${this.url}   to  ${backupDir} ...`);
        let backupResult = await secondaryNode.fullbackup({
            backup_dir: backupDir,
            db: backupInfo.db //需要备份的数据库
        });
        //let unLockRet = await secondaryNode.fsyncUnLock();
        //将最后的日志时间写入备份根目录
        console.log(`ReplicaSetDB fullbackup finish : ${this.url} ok, ${(new Date().getTime()-startTime)/1000}`);
        let statusFile = backupInfo.backup_dir + "/full/oplog_" + uriInfo.replSetName + ".json";
        console.log(`ReplicaSetDB write current oplog status : ${this.url}  ${oplogTime}..`);
        fs.writeFileSync(statusFile, `[${oplogTime.getLowBits()} , ${oplogTime.getHighBits()}]`);
        db.close();
        return Result.ok("ok");
    } catch (err) {
        //let unLockRet = secondaryNode.fsyncUnLock();
        let msg = `ReplicaSetDB fullbackup error ,  ${this.url} ${err.stack}`;
        console.log(msg)
        db.close();
        throw new Error(msg)
    }
}


ReplicaSetDB.prototype.incbackup = async function(backupInfo) {

    let startTime = new Date().getTime();
    let db = await this.getDb();
    let secondaryNode = await this.getSecondaryNode()
    console.log(`incbackup ${secondaryNode.url} start ...`)
    let uriInfo = this.getUriInfo();
    try {
        //读取上次增量被封的log的时间
        //如果没有增量信息，则抛出错误
        let lastTime = require(backupInfo.backup_dir + "/full/oplog_" + uriInfo.replSetName + ".json")
        if (!lastTime) {
            throw new Error("no oplog position")
        }
        //let lockRet = await secondaryNode.fsyncLock(); //枷锁数据
        let currentOplogTime = await secondaryNode.oplogTimestamp(); //当前数据节点的最后log时间
        if (!lastTime) {
            return Result.fail("没有oplog时间")
        }
        let backupInfo = {
            backup_dir: backupInfo.backup_dir + "/inc-" + currentOplogTime.getLowBits() + '_' + currentOplogTime.getHighBits(), //增量备份目录,时间是读取的最后时间
            lastTimestamp: new Timestamp(lastTime[0], lastTime[1]) //最后读取的时间
        }
        console.log("inc backup info ", backupInfo);
        let backupResult = await secondaryNode.incbackup(backupInfo);
        //let unLockRet = await secondaryNode.fsyncUnLock();
        //保存当前状态到文件
        let msg = `ReplicaSetDB incbackup finish , ${this.url} , ${(new Date().getTime()-startTime)/1000}`;
        console.log(msg);
        db.close();
        return Result.ok(msg)
    } catch (err) {
        let msg = `ReplicaSetDB incbackup fail ${this.url} , ${err.stack}`;
        console.log(msg);
        //let unLockRet = secondaryNode.fsyncUnLock();
        db.close();
        throw new Error(msg)
    }
}

/**
 * 得到复制集中的hide node 或 SECONDARY node
 */
ReplicaSetDB.prototype.getSecondaryNode = async function() {

    let db = await this.getDb();
    try {
        //得到复制集的信息，能够找出hidden节点
        let replSetConfig = await db.command({ replSetGetConfig: 1 })
        let configMembers = replSetConfig.config.members;
        let replSetStatus = await db.command({ replSetGetStatus: 1 });
        let stateMember = replSetStatus.members;

        let secondaryNode = null;
        let stateMemberMap = {};
        stateMember.forEach(function(member) {
            stateMemberMap[member._id] = member;
        })
        let uriInfo = this.getUriInfo();
        let masterUrl = ''
        configMembers.forEach((member) => {
            let priority = member.priority;
            let hidden = member.hidden;
            let _id = member._id;
            let host = member.host;
            if (stateMemberMap[_id] && stateMemberMap[_id].stateStr != "PRIMARY") { //不是关键节点
                let node = null;
                if (uriInfo.username) {
                    node = new Node(host, uriInfo.username, uriInfo.password);
                } else {
                    node = new Node(host, null, null);
                }
                if (hidden == true && priority == 0) { //找到一个hidden节点
                    secondaryNode = node;
                }
                secondaryNode = node;
            }
            if (stateMemberMap[_id] && stateMemberMap[_id].stateStr == "PRIMARY") {
                masterUrl = host;
            }
        });
        secondaryNode.setMaster(masterUrl)
        db.close();
        return secondaryNode;
    } catch (err) {
        let msg = `ReplicaSetDB ${this.url} getSecondaryNode error . ${err.stack}`;
        console.log(msg);
        db.close();
        throw new Error(msg)
    }
};

module.exports = ReplicaSetDB;