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
    let ret = /mongodb:\/\/(?:(.*):(.*)@)?([^?]*)(?:\?replicaSet\=(.*))?/gi.exec(this.url)
    return {
        username: ret[1],
        password: ret[2],
        ips: ret[3].split(','),
        replSetName: ret[4]
    }
}

module.exports = DB;