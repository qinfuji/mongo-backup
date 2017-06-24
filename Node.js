/**
 * 复制集中的一个节点
 */
const MongoClient = require('mongodb').MongoClient
const Timestamp = require('mongodb').Timestamp
const { exec } = require('child_process');
const Result = require("./Result");
//const moment = require("./moment");
/**
 * 复制集中的一个二级节点
 * @url 节点的地址
 * @master 节点的master地址
 * 
 */
function Node(url, username, password) {
    this.url = url;
    this.username = username
    this.password = password
};

Node.prototype.setMaster = function(masterUrl) {
    this.master = masterUrl;
}

/**
 * 枷锁当前节点
 */
Node.prototype.fsyncLock = async function() {
    return new Promise((resolve, reject) => {
        console.log(`fsyncLock ${this.url} start ...`);
        let commandLine = 'mongo ' + this.url + '/admin ' + this.getAuthParam() +
            " --quiet --eval 'rs.slaveOk();db.fsyncLock();db.currentOp();' | grep fsyncLock | grep true "
        console.log(commandLine)
        exec(commandLine, (error, stdout, stderr) => {
            console.log(`stdout: ${stdout}`);
            console.log(`stderr: ${stderr}`);
            if (error) {
                reject(Result.ok(`fsyncLock ${this.url} error: ${error}`));
                return;
            }
            if (stdout && stdout.match(/fsyncLock.*true/) /*.match(stdout)*/ ) {
                console.log(`fsyncLock ${this.url} ok!`);
                resolve(Result.ok(`fsyncLock ${this.url} ok!`));
            } else {
                reject(Result.fail(`fsyncLock ${this.url} fail ${stdout}`))
            }
        })
    })
}

Node.prototype.getAuthParam = function() {
    let auth = ""
    auth += (this.username ? " -u " + this.username : " ")
    auth += (this.password ? " -p " + this.password : " ")
    auth += ((this.username && this.password) ? " --authenticationDatabase admin " : "")
    return auth;
}


/**
 * 解锁当前节点
 */
Node.prototype.fsyncUnLock = async function() {
    return new Promise((resolve, reject) => {
        let commandLine = 'mongo ' + this.url + '/admin ' + this.getAuthParam() +
            " --eval 'db.fsyncUnlock()'" +
            " | grep 'fsyncUnlock completed' | grep ok "
        exec(commandLine, (error, stdout, stderr) => {
            console.log(`error: ${error}`);
            console.log(`stdout: ${stdout}`);
            console.log(`stderr: ${stderr}`);
            if (error) {
                reject(Result.fail(`fsyncUnLock ${this.url} error: ${error}`))
                return;
            }
            if (stdout && stdout.match(/fsyncUnlock completed/)) {
                console.log(`fsyncUnLock ${this.url}  ok !`)
                resolve(Result.ok(`fsyncUnLock ${this.url}  ok !`));
            } else {
                console.log(`fsyncUnLock ${this.url}  fail !`)
                reject(Result.fail(`fsyncUnLock ${this.url}  fail!  ${stdout}\n${stderr}`))
            }
        })
    })
}

/**
 * 全量备份当前节点数据
 * @param backupInfo 备份相关信息
 */
Node.prototype.fullbackup = async function(backupInfo) {

    if (!backupInfo) {
        throw new Error(`fullbackup ${this.url} 备份选项必须设置`);
    }
    cmd_dump = "mongodump " +
        " --host " + this.url +
        this.getAuthParam() +
        (backupInfo.db ? " --db " + backupInfo.db : " ") +
        " --out " + backupInfo.backup_dir
    await dump(cmd_dump, this);
}

/**
 * 增量备份当前节点信息
 * @param backupInfo 备份相关信息
 *        {Timestamp}backupInfo.inc_timestamp  增量的时间点
 *        {string}backupInfo.inc_backup_dir 增量备份的目录
 */
Node.prototype.incbackup = async function(backupInfo) {
    //从备份目录中获取上次备份信息
    if (!backupInfo) {
        throw new Error(`incbackup ${this.url} 备份选项必须设置`);
    }
    let timestamp = backupInfo.lastTimestamp;
    if (!timestamp) {
        throw new Error(`incbackup ${this.url} 没有增量的时间点`);
    }
    let ns = ""
    if (backupInfo.db) {
        ns += `, "ns": /${backupInfo.db}/`
    }
    let query = `'{ "ts": { "$gte": Timestamp(${timestamp.getLowBits()}, ${timestamp.getHighBits()})${ns}}}'`
    let cmd_dump = "mongodump" +
        this.getAuthParam() +
        " --host " + this.url + " --out " + backupInfo.backup_dir +
        " --db local --collection oplog.rs --query " + query
    return dump(cmd_dump, this)
}


Node.prototype.oplogTimestamp = async function() {
    let db = await this.connect();
    let localdb = await db.db("local")
    let oplog = await localdb.collection("oplog.rs");
    let lastLog = await oplog.find().sort({ $natural: -1 }).limit(1).toArray();
    let timestamp = lastLog[0].ts; //最后的时间
    db.close();
    return timestamp;
}

Node.prototype.connect = async function() {
    if (!this.db) {
        let url = "mongodb://"
        if (this.username && this.password) {
            url += this.username + ":" + this.password + "@"
        }
        url += this.master;
        this.db = await MongoClient.connect(url);
    }
    return this.db;
}

async function dump(cmd, db) {
    console.log(cmd)
    return new Promise((resolve, reject) => {
        exec(cmd, { maxBuffer: 5000 * 1024 }, (error, stdout, stderr) => {
            console.log(`stdout: ${stdout}`);
            console.log(`stderr: ${stderr}`);
            if (error) {
                console.log(`dump ${db.url} error: ${ error }`)
                reject(Result.fail(`dump ${db.url} error: ${ error }`))
                return;
            }
            console.log(`dump ${db.url} ok`)
            resolve(Result.ok(`dump ${db.url} ok`))
        })
    })
}

module.exports = Node