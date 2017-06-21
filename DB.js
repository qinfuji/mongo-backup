const MongoClient = require('mongodb').MongoClient

function DB(url) {
    this.url = url;
}


DB.prototype.getDb = async function() {
    if (!this.db) {
        this.db = await MongoClient.connect(this.url);
    }
    return this.db;
}

DB.prototype.getUriInfo = function() {
    let startIdx = this.url.index("mongodb://")
    let endIdx = this.url.index("@")
    let ret = /mongodb:\/\/(?:(.*):(.*)@)?([^?]*)(?:\?replSetName\=(.*))?/gi.exec(this.url)
    return {
        username: ret[0],
        password: ret[1],
        ips: ret[2].split(','),
        replSetName: ret[3]
    }
}

module.exports = DB;