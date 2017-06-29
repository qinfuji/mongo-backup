/**
 * 检测数据的正确性
 */

const MongoClient = require('mongodb').MongoClient
const ObjectID = require('mongodb').ObjectID;
const moment = require("moment");
const program = require('commander');

console.log("------------------------------------------------------------------------");
console.log("------------------------------------------------------------------------");


/**
 * 检测文章数据数量
 */
async function check(startDate, endDate, sourceDBUrl, targetDBUrl) {

    console.log(arguments)
    let oldDB = await MongoClient.connect(sourceDBUrl);
    let newDB = await MongoClient.connect(targetDBUrl);
    try {
        let oArtialeCol = oldDB.collection("article");
        let nArtialeCol = newDB.collection("article");
        let cursor = oArtialeCol.find({ $and: [{ importTime: { $gte: startDate.toDate() } }, { importTime: { $lte: endDate.toDate() } }] }, { importTime: 1 })
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

program
    .version('0.0.1')
    .option('--sourceuri [value]', '源数据库URL', '')
    .option('--targeturi [value]', '被检测数据库URL', '')
program.parse(process.argv);

let startDate = moment();
startDate.add(-60, 'minutes');
let endDate = moment();
endDate.add(-30, 'minutes');
check(startDate, endDate, program.sourceuri, program.targeturi).then({}).catch(function(err) {
    console.log(err)
})