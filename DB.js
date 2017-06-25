const MongoClient = require('mongodb').MongoClient

function DB(url) {
    this.url = url;
}


DB.prototype.getDb = async function() {
    return await MongoClient.connect(this.url);
}

module.exports = DB;