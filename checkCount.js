/**
 * 检测数据的正确性
 */

const MongoClient = require('mongodb').MongoClient
const ObjectID = require('mongodb').ObjectID;

async function check(startDate) {
    let oldDB = await MongoClient.connect("mongodb://10.90.13.157:27017,10.90.13.158:27017/fhh");
    let newDB = await MongoClient.connect("mongodb://fhh_rw_p:fhh_rw_p@10.90.13.159:27017,10.90.13.160:27017,10.90.13.161:27017/fhh?replicaSet=fhhReplSet");
    try {


        let oArtialeCol = oldDB.collection("article");
        let nArtialeCol = newDB.collection("article");
        let cursor = oArtialeCol.find({ $and: [{ importTime: { $gte: new Date("2017-06-28 13:00:00") } }, { importTime: { $lt: new Date("2017-06-28 23:59:59") } }] }, { importTime: -1, eArticleId: 1, title: 1 })
        let count = 0;
        while (await cursor.hasNext()) {
            let item = await cursor.next();
            let nItem = await nArtialeCol.findOne({ _id: item._id })
            if (!nItem) {
                console.log(++count, item.importTime, item._id);
            }
            hasNext = await cursor.hasNext();
        }
        oldDB.close();
        newDB.close();
        console.log("ok")
    } catch (err) {
        oldDB.close();
        newDB.close();
        console.log(err)
        throw err
    }
}

check(new Date("2017-06-28 00:00:00")).then({

}).catch(function(err) {
    console.log(err)
})