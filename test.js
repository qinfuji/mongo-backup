// const ShardingDB = require("./ShardingDB")
// let sdb = new ShardingDB("mongodb://fhh_super:fhh_super@10.90.34.36:27047")


// sdb.getReplSetDB().then(function(result) {
//     console.log(result)
// }).catch(function(result) {
//     console.log(result)
// });
// sdb.getBackupNode().then(function(result) {
//     console.log(result)
//     sdb.close()
// }).catch(function(err) {
//     console.log(err.stack)
//     sdb.close()
// })

// const ShardingDB = require("./ShardingDB")
// let sdb = new ShardingDB("mongodb://10.90.13.157:27017,10.90.13.158:27017")


//const MongoClient = require('mongodb').MongoClient;
// MongoClient.connect("mongodb://10.90.9.25:27017/?replicaSet=fhhshard1", function(err, db) {
//     //console.log(err, db)
//     let localdb = db.db("local")
//     let oplogs = localdb.collection("oplog.rs")
//     plog.find().sort({ $natural: -1 }).limit(1).toArray();
// });

// (async function() {
//     let db = await MongoClient.connect("mongodb://fhh_super_p:fhh_super_p@10.90.13.160:27017/?replicaSet=fhhReplSet");
//     let localdb = await db.db("local")
//     let oplog = await localdb.collection("oplog.rs");
//     let lastLog = await oplog.find().sort({ $natural: -1 }).limit(1).toArray();
//     let timestamp = lastLog[0].ts; //最后的时间
//     console.log(timestamp);
//     await new Promise(function() {});
// })();




// sdb.getSecondaryNode().then(function(result) {
//     console.log(result)
//     sdb.close()
// }).catch(function(err) {
//     console.log(err.stack)
//     sdb.close()
// })


// sdb.fullbackup(__dirname).then(function(result) {
//     console.log("fullbackup finish ", result)
// }).catch(function(err) {
//     console.log("fullbackup error ", err.stack)
// })

// sdb.incbackup(__dirname).then(function(result) {
//     console.log("incbackup finish ", result)
// }).catch(function(err) {
//     console.log("incbackup error ", err.stack)
// })

// sdb.getReplSetDB().then((result) => {
//     if (result && result.length) {
//         result.forEach(function(db) {
//             // db.getSecondaryNode().then(function(result) {
//             //     console.log(result)
//             // }).catch(function(err) {
//             //     console.log(err);
//             // })
//             db.fullbackup({
//                 backup_dir: "."
//             })
//         })
//     }
// }).catch(function(err) {
//     console.log(err)
// })


// const MongoClient = require('mongodb').MongoClient

// MongoClient.connect("mongodb://10.90.34.38:27037").then(function(db) {
//     console.log(db);
// })

// const Node = require("./Node");
// const Timestamp = require('mongodb').Timestamp
// let node = new Node("10.90.34.36:27017", "10.90.34.37:27017", "fhh_super", "fhh_super");


// async function start() {
//     const Node = require("./Node");
//     let node = new Node("10.90.34.36:27017", "10.90.34.37:27017", "fhh_super", "fhh_super");
//     try {

//         //await node.fsyncLock();
//         let timestamp = await node.oplogTimestamp();
//         console.log(timestamp.getHighBits());
//         let backupOptions = {
//             inc_backup_dir: ".",
//             inc_timestamp: new Timestamp(timestamp.getHighBits(), 1)
//         };
//         console.log(timestamp)
//         await node.incbackup(backupOptions)
//             //await node.fsyncUnLock();
//         return true

//     } catch (err) {
//         //node.fsyncUnLock();
//         console.log(err.stack)
//         return false;
//     }
// }

// start()

// function A() {}

// A.prototype.m1 = async function() {
//     console.log("A m1")
// }

// A.prototype.m2 = async function() {
//     return "A m2"
// }


// function B() {
//     this.ai = new A();
// }

// B.prototype.m1 = async function() {
//     await this.ai.m1();
//     console.log("B m1")
// }

// B.prototype.m2 = async function() {
//     await this.ai.m2();
//     console.log("B m2");
// }

// let bi = new B();

// bi.m1();


// const Timestamp = require('mongodb').Timestamp
// const fs = require("fs")
// let timestamp = new Timestamp(1497964171, 1);
// console.log(timestamp)
// fs.writeFileSync("c:\\sss", "const Timestamp = require('mongodb').Timestamp;module.exports.lastTime = new  Timestamp(" + timestamp.getLowBits() + "," + timestamp.getHighBits() + ")")

// fs.writeFileSync("c:\\timestamp.json", "[" + timestamp.getLowBits() + "," + timestamp.getHighBits() + "]");

// var re = require("c:\\timestamp.json")
// console.log(re);

// const oplogs = require("./Oplog");
// const path = require("path")
// oplogs.merge(path.join(__dirname, "oplogfiles/out_oplog.rs.bson"), [path.join(__dirname, "oplogfiles/oplog.rs_1.bson"), path.join(__dirname, "oplogfiles/oplog.rs.bson")])

// const MongoClient = require('mongodb').MongoClient;

// (async function start() {

//     let db = await MongoClient.connect("mongodb://fhh_rw_p:fhh_rw_p@10.90.13.159:27017,10.90.13.160:27017,10.90.13.161:27017/fhh?replicaSet=fhhReplSet")
//     let articleColl = db.collection("article");
//     let docs = await articleColl.find({ importTime: { $gt: new Date("2017-07-11 11:05:00") }, eArticleId: { $type: 16 } });
//     //console.log(docs)
//     //let hasNext = await docs.hasNext();
//     //console.log(hasNext);
//     let count = 0;
//     console.log("{")
//     while (await docs.hasNext()) {
//         let doc = await docs.next();
//         console.log("'" + doc.eArticleId + "':true,");

//     }
//     console.log("}")
//     db.close();
// })()


const fs = require("fs")
const path = require("path");
const BSON = require('bson');
const errids = require("./errids").ids;
const Timestamp = require('mongodb').Timestamp;
var bson = new BSON();

let contentBuffer = fs.readFileSync(path.join(__dirname, "oplogfiles", "oplog.bson"));

let bufIdx = 0;
const docCount = 1;
let docIdx = 0;
let lastDoc = null;
while (bufIdx < contentBuffer.length) {
    let docs = []
    let retIdx = bson.deserializeStream(contentBuffer, bufIdx, docCount, docs, docIdx);
    let curDoc = docs[0];
    // let op = curDoc.op;
    // //if (op == 'i' && curDoc.ns == 'fhh.article' && errids[curDoc.o.eArticleId]) { //新增操作
    // console.log(curDoc.ts);
    // //}

    if (!lastDoc) {
        lastDoc = curDoc;
    } else {
        let l = new Timestamp(lastDoc.ts.low_, lastDoc.ts.high_);
        let c = new Timestamp(curDoc.ts.low_, curDoc.ts.high_);
        let ret = l.compare(c)
        if (ret >= 1) {
            throw new Error("oplog time error");
        }
        lastDoc = curDoc;
    }
    bufIdx = retIdx;
}