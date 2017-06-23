const MongoClient = require('mongodb').MongoClient
const DB = require("./DB")
const ReplicaSetDB = require("./ReplicaSetDB")
const Result = require("./Result");

function ShardingDB(url) {
    DB.apply(this, [url]);
}

ShardingDB.prototype.constructor = ShardingDB;
ShardingDB.prototype = Object.create(DB.prototype);


ShardingDB.prototype.stopBalance = async function() {
    let db = await this.getDb();
    setBanlance(db, true);
    db.close();
}

ShardingDB.prototype.startBalance = async function() {
    let db = await this.gteDb();
    setBanlance(db, false);
    db.close();
}

/**
 * 全量备份
 */
ShardingDB.prototype.fullbackup = async function(backupInfo) {
    try {
        //await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        console.log(`ShardingDB getReplSetDB ${this.url} replSets ${replSets}`)
        let waitBackupReplSet = [];
        replSets.forEach(function(replSet) {
            let _r = await replSet.fullbackup(backupInfo);
            console.log(_r);
            waitBackupReplSet.push(_r);
        })
        let self = this;
        await waitBackupReplSet;
        let msg = `fullbackup  ${this.url}  finish`;
        console.log(msg);
        return Result.ok(msg);
        //return self.stopBalance(); //启动集群负载均衡
    } catch (err) {
        //await this.startBalance();
        throw new Error("sharding fullbackup fail", err.stack)
    }
}

/**
 * 增量备份
 */
ShardingDB.prototype.incbackup = async function(backupInfo) {
    try {
        console.log(`start incbackup ${this.url} ...`);
        //await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        let waitBackupReplSet = [];
        replSets.forEach(function(replSet) {
            let _r = replSet.fullbackup(backupInfo);
            waitBackupReplSet.push(_r);
        })
        let self = this;
        await waitBackupReplSet;
        console.log(`incbackup finish : ${this.url}`)
        console.log(`close db : ${this.url}`)
        this.close();
        return Result.ok(`incbackup finish : ${this.url}`);
        //return self.stopBalance(); //启动集群负载均衡
    } catch (err) {
        //await this.startBalance();
        let msg = `ShardingDB incbackup error . ${this.url} , ${err.stack}`
        console.log(msg)
        throw new Error(msg);
    }
}


/**
 * 得到复制集
 */
ShardingDB.prototype.getReplSetDB = async function() {

    let db = await this.getDb();
    try {
        let configDB = db.db("config")
        let shardolleciton = await configDB.collection("shards");
        let shardInfos = await shardolleciton.find({}).toArray();
        let replicaSets = [];
        let uriInfo = this.getUriInfo();
        shardInfos.forEach((shardInfo) => {
            let shardhost = shardInfo.host;
            let ips = shardhost.split("/");
            let url = "mongodb://";
            if (uriInfo.username) {
                url += uriInfo.username + ":" + uriInfo.password + "@"
            }
            url += ips[1] + "?replicaSet=" + ips[0];
            let replSetDB = new ReplicaSetDB(url);
            replicaSets.push(replSetDB);
        })
        db.close();
        return replicaSets
    } catch (err) {
        let msg = `ShardingDB ${this.url} getReplSetDB error . ${err.stack}`;
        console.log(msg)
        db.close();
        throw new Error(msg)
    }
}

async function setBanlance(db, state) {
    let configdb = db.db("config");
    let config_col = configdb.collection("settings")
    return config_col.update({ _id: "balance" }, { '$set': { 'stopped': state } });
}

module.exports = ShardingDB;