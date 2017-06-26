var Node = require("./Node")
const DB = require("./DB")
var fs = require('fs');
var Result = require("./Result")
const Timestamp = require('mongodb').Timestamp;
const { getUriInfo } = require("./utils");
const path = require("path");
var mkdirp = require('mkdirp');

function ReplicaSetDB(url) {
    DB.apply(this, [url]);
}

ReplicaSetDB.prototype.constructor = ReplicaSetDB;
ReplicaSetDB.prototype = Object.create(DB.prototype);


ReplicaSetDB.prototype.fullbackup = async function({ backupdir, backupdb }) {

    let db = await this.getDb();
    let uriInfo = getUriInfo(this.url);
    let secondaryNode = await this.getSecondaryNode()
    console.log(`ReplicaSetDB fullbackup ${this.url} : ${secondaryNode.toString()}`)
    let fullBackdir = path.join(backupdir, "full");
    let statusFile = path.join(backupdir, "oplog_" + uriInfo.replSetName + "_status.json");
    mkdirp.sync(fullBackdir);
    try {
        let startTime = new Date().getTime();
        //let lockRet = await secondaryNode.fsyncLock(); //加入锁
        let oplogTime = await secondaryNode.oplogTimestamp() //得到最后的oplog时间
        let replSetBackupDir = path.join(fullBackdir, uriInfo.replSetName); //全量备份目录
        console.log(`start backup  ReplicaSetDB ${this.url}   to  ${replSetBackupDir} ...`);

        let backupResult = await secondaryNode.fullbackup({
            backup_dir: replSetBackupDir,
            db: backupdb
        });
        //let unLockRet = await secondaryNode.fsyncUnLock();
        //将最后的日志时间写入备份根目录
        console.log(`ReplicaSetDB fullbackup finish : ${this.url} ok, ${(new Date().getTime()-startTime)/1000}`);
        console.log(`ReplicaSetDB write current oplog status : ${this.url}  ${oplogTime}..`);
        fs.writeFileSync(statusFile, `[${oplogTime.getHighBits()} , ${oplogTime.getLowBits()}]`);
        db.close();
        return {
            finishDir: replSetBackupDir,
            statusFile: statusFile
        }
    } catch (err) {
        //let unLockRet = secondaryNode.fsyncUnLock();
        let msg = `ReplicaSetDB fullbackup error ,  ${this.url} ${err.stack}`;
        console.log(msg)
        db.close();
        throw new Error(msg)
    }
}


ReplicaSetDB.prototype.incbackup = async function({ backupdir, backupdb }) {

    let startTime = new Date().getTime();
    let db = await this.getDb();
    let secondaryNode = await this.getSecondaryNode()
    console.log(`incbackup ${secondaryNode.url} start ...`)
    let uriInfo = getUriInfo(this.url);
    let statusFile = path.join(backupdir, "oplog_" + uriInfo.replSetName + "_status.json");
    try {
        //读取上次增量被封的log的时间
        //如果没有增量信息，则抛出错误
        let lastTime = require(statusFile)
        if (!lastTime) {
            throw new Error("no oplog position")
        }
        let lastTimestamp = new Timestamp(lastTime[1], lastTime[0])
            //let lockRet = await secondaryNode.fsyncLock(); //枷锁数据
        let currentOplogTime = await secondaryNode.oplogTimestamp(); //当前数据节点的最后log时间
        if (!lastTime) {
            return Result.fail("没有oplog时间")
        }
        //设置目录后缀
        let incSuffix = lastTimestamp.getHighBits() + "_" + lastTimestamp.getLowBits() + "_" + currentOplogTime.getHighBits() + "_" + currentOplogTime.getLowBits()
        let replSetBackupDir = path.join(backupdir, "inc", uriInfo.replSetName + "_" + incSuffix);
        let _backupInfo = {
            backup_dir: replSetBackupDir, //增量备份目录,时间是读取的最后时间
            lastTimestamp: lastTimestamp, //最后读取的时间
            db: backupdb //要备份的数据库
        }
        console.log("inc backup info ", _backupInfo);
        let backupResult = await secondaryNode.incbackup(_backupInfo);
        //let unLockRet = await secondaryNode.fsyncUnLock();
        //保存当前状态到文件
        console.log(`ReplicaSetDB write current oplog status : ${this.url}  ${currentOplogTime}..`);
        fs.writeFileSync(statusFile, `[${currentOplogTime.getHighBits()} , ${currentOplogTime.getLowBits()}]`);
        let msg = `ReplicaSetDB incbackup finish , ${this.url} , ${(new Date().getTime()-startTime)/1000}`;
        console.log(msg);
        db.close();
        //return Result.ok("ok")
        return {
            finishDir: replSetBackupDir,
            startTimestamp: lastTime,
            endTimestamp: currentOplogTime,
            statusFile: statusFile
        }
    } catch (err) {
        let msg = `ReplicaSetDB incbackup fail ${this.url} , ${err.stack}`;
        console.log(msg);
        //let unLockRet = secondaryNode.fsyncUnLock();
        db.close();
        throw new Error(msg)
    }
}

/**
 * 从指定的目录恢复数据
 */
ReplicaSetDB.prototype.fullRestore = async function(resotreInfo) {
    //获取主节点
    let oneNode = await this.getSecondaryNode();
    return oneNode.fullRestore(resotreInfo);
}

ReplicaSetDB.prototype.incRestore = async function(resotreInfo) {
    let oneNode = await this.getSecondaryNode();
    return oneNode.incRestore(resotreInfo);
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
        let uriInfo = getUriInfo(this.url);
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