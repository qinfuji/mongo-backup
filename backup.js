/**
 * mongo 数据迁移
 */

var program = require('commander');
var { cmdExe } = require("./utils");
var path = require("path");

program
    .version('0.0.1')
    .option('--deploy [value]', '目标数据库模式，replSet|sharding', '')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--mode [value]', 'full|inc', '')
program.parse(process.argv);


if (!(program.backupdir && program.deploy && program.uri && program.mode)) {
    program.help()
    return;
}

let BackupDB = null;
if (program.deploy == 'replSet') {
    BackupDB = require("./ReplicaSetDB")
} else if (program.deploy == 'sharding') {
    BackupDB = require("./ShardingDB")
}

async function back() {
    let backupDB = new BackupDB(program.uri);
    try {
        if (program.mode == 'full') {
            let fullRetInfos = await backupDB.fullbackup(program.backupdir);
        }
        let incRetInfos = await backupDB.incbackup(program.backupdir);
        console.log("---->", incRetInfos);
        //重新整理路径
        if (!Array.isArray(incRetInfos)) {
            //如果是多个增量文件，并且将增量文件移动到指定目录
            incRetInfos = [incRetInfos];
        }
        incRetInfos.forEach(function(incRetInfo) {
            let finishDir = incRetInfo.finishDir
            let baseName = path.baseName(finishDir);
            let cmd_line = `cp -P ${findshDir}/local/oplog.rs.bson ${program.backupdir}/incfinish/oplog.rs_${baseName}.bson`;
            cmdExe(cmd_line).then(function() {}).catch(function(err) {
                console.log(err, err.stack)
            })
        })
        return;
    } catch (err) {
        console.log(err, err.stack);
    }
}

back();