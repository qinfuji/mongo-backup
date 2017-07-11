const fs = require("fs");
const BSON = require('bson');
const Timestamp = require('mongodb').Timestamp;
const path = require("path");


function Oplogs() {}

module.exports.merge = function(outputFile, filenames) {

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
    let unDeserializeDoc = {};
    while (bufIdx < contentBuffer.length) {
        let docs = []
        let retIdx = bson.deserializeStream(contentBuffer, bufIdx, docCount, docs, docIdx);
        let docsBuffer = contentBuffer.slice(bufIdx, retIdx);
        //console.log(docs[0])
        deserDocuments.push(docs[0]);
        unDeserializeDoc[docs[0].h] = (docsBuffer);
        bufIdx = retIdx;
    }

    deserDocuments.sort((a, b) => {
        let ts1 = new Timestamp(a.ts.low_, a.ts.high_);
        let ts2 = new Timestamp(b.ts.low_, b.ts.high_);
        return ts1.compare(ts2);
    })

    let rets = [];
    deserDocuments.forEach(function(doc) {
        //let b = bson.serialize(doc);
        let b = unDeserializeDoc[doc.h];
        rets.push(b);
    });
    console.log(rets.length);
    let allbuff = Buffer.concat(rets);
    fs.writeFileSync(outputFile, allbuff);
    return outputFile;
}


/**
 * 验证合并后的文件，保证在时间上是升序的
 */
module.exports.vailde = function(filename) {
    var bson = new BSON();
    let contentBuffer = fs.readFileSync(filename)
    let bufIdx = 0;
    const docCount = 1;
    let docIdx = 0;
    let lastDoc = null;
    while (bufIdx < contentBuffer.length) {
        let docs = []
        let retIdx = bson.deserializeStream(contentBuffer, bufIdx, docCount, docs, docIdx);
        let curDoc = docs[0];
        if (!lastDoc) {
            lastDoc = curDoc;
        } else {
            let l = new Timestamp(lastDoc.ts.low_, lastDoc.ts.high_);
            let c = new Timestamp(curDoc.ts.low_, curDoc.ts.high_);
            let ret = l.compare(c)
            if (ret >= 1) {
                throw new Error("oplog time error");
            }
            curDoc = curDoc;
        }
        bufIdx = retIdx;
    }
}