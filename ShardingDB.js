const MongoClient = require('mongodb').MongoClient
const DB = require("./DB")
const ReplicaSetDB = require("./ReplicaSetDB")
const Result = require("./Result");
const { getUriInfo } = require("./utils");
const path = require("path")

function ShardingDB(url) {
    DB.apply(this, [url]);
}

ShardingDB.prototype.constructor = ShardingDB;
ShardingDB.prototype = Object.create(DB.prototype);


ShardingDB.prototype.stopBalance = async function() {
    let db = await this.getDb();
    await setBanlance(db, true);
    db.close();
}

ShardingDB.prototype.startBalance = async function() {
    let db = await this.getDb();
    await setBanlance(db, false);
    db.close();
}

/**
 * 全量备份
 */
ShardingDB.prototype.fullbackup = async function({ backupdir, backupdb }) {
    try {
        let startTime = new Date().getTime();
        await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        console.log(`ShardingDB getReplSetDB ${this.url} replSets ${replSets.toString()}`)
        let waitBackupReplSet = [];

        for (var i = 0; i < replSets.length; i++) {
            let _r = replSets[i].fullbackup({ backupdir, backupdb });
            waitBackupReplSet.push(_r);
        };
        // let ret = await waitBackupReplSet;
        // await this.startBalance(); //启动集群负载均衡
        // let msg = `ShardingDB fullbackup  ${this.url}  finish. ${(new Date().getTime()-startTime)/1000}`;
        // console.log(msg);
        // return ret;
        let self = this;
        return await Promise.all(waitBackupReplSet).then((result) => {
            return (async() => {
                let msg = `ShardingDB fullbackup  ${this.url}  finish. ${(new Date().getTime()-startTime)/1000}`;
                console.log(msg);
                await self.startBalance(); //启动集群负载均衡
                return result;
            })();
        }).catch((err) => {
            return (async() => {
                await self.startBalance(); //启动集群负载均衡
                throw new Error(`sharding fullbackup fail , ${this.url} , ${err} , ${err.stack}`)
            })();
        });
    } catch (err) {
        await this.startBalance();
        throw new Error(`sharding fullbackup fail, ${this.url} , ${err} ,${err}`);
    }
}

/**
 * 增量备份
 */
ShardingDB.prototype.incbackup = async function({ backupdir, backupdb }) {
    try {
        console.log(`start incbackup ${this.url} ...`);
        let startTime = new Date().getTime();
        await this.stopBalance(); //停止集群负载均衡
        let replSets = await this.getReplSetDB(); //得到集群中的所有复制集
        let waitBackupReplSet = [];
        replSets.forEach(function(replSet) {
            let _r = replSet.incbackup({ backupdir, backupdb });
            waitBackupReplSet.push(_r);
        });
        // let ret = await waitBackupReplSet;
        // await this.startBalance(); //启动集群负载均衡
        // let msg = `ShardingDB incbackup  ${this.url}  finish. ${(new Date().getTime()-startTime)/1000}`;
        // console.log(msg);
        // return ret;
        let self = this;
        return await Promise.all(waitBackupReplSet).then((result) => {
            return (async() => {
                let msg = `ShardingDB incbackup  ${self.url}  finish. ${(new Date().getTime()-startTime)/1000}`;
                console.log(msg);
                await self.startBalance(); //启动集群负载均衡
                return result;
            })();
        }).catch((err) => {
            return (async() => {
                await self.startBalance(); //启动集群负载均衡
                throw new Error(`sharding incbackup fail , ${self.url} , ${err}, ${err.stack}`)
            })();
        });
    } catch (err) {
        await this.startBalance();
        let msg = `ShardingDB incbackup error . ${this.url} , ${err} , ${err.stack}`
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
        let uriInfo = getUriInfo(this.url);
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
    return await config_col.update({ _id: "balancer" }, { '$set': { 'stopped': state } });
}

module.exports = ShardingDB;