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
        let cursor = oArtialeCol.find({ $and: [{ importTime: { $gte: startDate.toDate() } }, { importTime: { $lte: endDate.toDate() } }] } /*, { importTime: 1 }*/ )
        let count = 0;
        let diffData = [];
        while (await cursor.hasNext()) {
            let item = await cursor.next();
            let nItem = await nArtialeCol.findOne({ _id: item._id })
            if (!nItem) {
                console.log(++count, item.importTime, item._id);
            } else {
                //验证数据一致性
                let str1 = JSON.stringify(item);
                let str2 = JSON.stringify(nItem);
                if (str1 != str2) {
                    console.log(str1)
                    console.log(str2);
                    diffData.push(nItem);
                }
            }
            hasNext = await cursor.hasNext();
        }
        console.log("diffData lenght", diffData.length);

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
    .option('--startDate [value]', '开始检查时间')
    .option('--endDate [value]', '开始检查时间')
program.parse(process.argv);

let startDate = moment();
if (program.startDate) {
    startDate = moment(program.startDate);
} else {
    startDate.add(-60, 'minutes');
}
let endDate = moment();
if (program.endDate) {
    endDate = moment(program.endDate);
} else {
    endDate.add(-30, 'minutes');
}
console.log("startDate", startDate.format("YYYY-MM-DD hh:mm:ss"));
console.log("endDate", endDate.format("YYYY-MM-DD hh:mm:ss"));
check(startDate, endDate, program.sourceuri, program.targeturi).then({}).catch(function(err) {
    console.log(err)
})