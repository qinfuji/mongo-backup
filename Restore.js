/**
 * 恢复数据
 */
const { exec } = require('child_process');

class Restore {

    /**
     * @param uri 待恢复数据的数据库url
     */
    constructor(url, username, password) {
        this.uri = uri;
        this.username = username;
        this.password = password;
    };
    /**
     * 增量恢复
     * @param restoreInfo 恢复信息
     */
    increstore(restoreInfo) {

    }

    /**
     * 全量恢复
     * @param restoreInfo 恢复信息
     */
    fullrestore(restoreInfo) {
        //假设备份的数据已经合并。
        let cmd_line = `mongorestore  --host 10.90.34.38 --port 27038 --username fhh_super --password fhh_super 
                 --authenticationDatabase admin --db fhh_test --drop --dir /data/mongodbback/full/2017-06-17/shard-2-test/fhh_test --noIndexRestore`;

        exec(cmd_line, (error, stdout, stderr) => {

        })
    }
}