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

const ShardingDB = require("./ShardingDB")
let sdb = new ShardingDB("mongodb://10.90.13.157:27017,10.90.13.158:27017")

// sdb.getSecondaryNode().then(function(result) {
//     console.log(result)
//     sdb.close()
// }).catch(function(err) {
//     console.log(err.stack)
//     sdb.close()
// })


// sdb.fullbackup({
//     backup_dir: __dirname
// }).then(function(result) {
//     console.log("fullbackup finish ", result)
// }).catch(function(err) {
//     console.log("fullbackup error ", err.stack)
// })

sdb.incbackup({
    backup_dir: __dirname
}).then(function(result) {
    console.log("incbackup finish ", result)
}).catch(function(err) {
    console.log("incbackup error ", err.stack)
})

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