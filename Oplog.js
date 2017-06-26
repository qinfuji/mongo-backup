const fs = require("fs");
const BSON = require('bson');
const Timestamp = require('mongodb').Timestamp;
const path = require("path");


function Oplogs() {}

module.exports.merge = function(...filenames, outputFile) {

    if (filenames.length == 0) {
        throw new Error("没有需要合并的文件！")
    }

    let contents = [];
    filenames.forEach(function(filename) {
        contents.push(fs.readFileSync(filename));
    })
    let contentBuffer = Buffer.concat(contents);
    var bson = new BSON();

    let bufIdx = 0;
    const docCount = 1;
    let docIdx = 0;
    let deserDocuments = [];
    while (bufIdx < contentBuffer.length) {
        let docs = []
        let retIdx = bson.deserializeStream(contentBuffer, bufIdx, docCount, docs, docIdx);
        deserDocuments.push(docs[0]);
        bufIdx = retIdx;
    }

    deserDocuments.sort((a, b) => {
        let ts1 = new Timestamp(a.ts.low_, a.ts.high_);
        let ts2 = new Timestamp(b.ts.low_, b.ts.high_);
        return ts1.compare(ts2);
    })

    let rets = [];
    deserDocuments.forEach(function(doc) {
        let b = bson.serialize(doc);
        rets.push(b);
    });
    console.log(rets.length);
    let allbuff = Buffer.concat(rets);
    //let outputFile = path.join(output, "./oplogs.bson");
    fs.writeFileSync(outputFile, allbuff);
    return outputFile;
}