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
    return setBanlance(db, true);
}

ShardingDB.prototype.startBalance = async function() {
    let db = await this.gteDb();
    return setBanlance(db, false);
}

/**
 * 全量备份
 */
ShardingDB.prototype.fullbackup = async function(backupInfo) {
    try {
        //await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        console.log(`ShardingDB ${this.url} replSets ${replSets}`)
        let waitBackupReplSet = [];
        replSets.forEach(function(replSet) {
            let _r = replSet.fullbackup(backupInfo);
            waitBackupReplSet.push(_r);
        })
        let self = this;
        await waitBackupReplSet;
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
        //await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        let waitBackupReplSet = [];
        replSets.forEach(function(replSet) {
            let _r = replSet.fullbackup(backupInfo);
            waitBackupReplSet.push(_r);
        })
        let self = this;
        await waitBackupReplSet;
        //return self.stopBalance(); //启动集群负载均衡
    } catch (err) {
        //await this.startBalance();
        throw new Error(err);
    }
}


ShardingDB.prototype.close = async function() {
    let db = await this.getDb();
    if (db) {
        db.close();
    }
}

/**
 * 得到复制集
 */
ShardingDB.prototype.getReplSetDB = async function() {

    let db = await this.getDb();
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
    return replicaSets
}

async function setBanlance(db, state) {
    let configdb = db.db("config");
    let config_col = configdb.collection("settings")
    return config_col.update({ _id: "balance" }, { '$set': { 'stopped': state } });
}

module.exports = ShardingDB;