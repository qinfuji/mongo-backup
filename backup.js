/**
 * mongo 数据迁移
 */

const program = require('commander');
const { cmdExe } = require("./utils");
const path = require("path");
const mkdirp = require('mkdirp');


console.log("------------------------------------------------------------------------");
console.log("------------------------------------------------------------------------");

program
    .version('0.0.1')
    .option('--deploy [value]', '目标数据库模式，replSet|sharding', '')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--db [value]', '要备份的数据库', '')
    .option('--mode [value]', 'full|inc', '')
program.parse(process.argv);


if (!(program.backupdir && program.deploy && program.uri && program.mode && program.db)) {
    program.help()
    return;
}

let BackupDB = null;
if (program.deploy == 'replSet') {
    BackupDB = require("./ReplicaSetDB")
} else if (program.deploy == 'sharding') {
    BackupDB = require("./ShardingDB")
}
let backupDB = new BackupDB(program.uri);

async function backup({ backupdir, db }) {
    if (!db) {
        db = "";
    }
    try {
        if (program.mode == 'full') {
            let fullRetInfos = await backupDB.fullbackup({
                backupdir: backupdir,
                backupdb: db
            });
        } else if (program.mode == 'inc') {
            let incRetInfos = await backupDB.incbackup({
                backupdir: backupdir,
                backupdb: db
            });
            //console.log("---------------->", incRetInfos);
            //重新整理路径
            if (!Array.isArray(incRetInfos)) {
                //如果是多个增量文件，并且将增量文件移动到指定目录
                incRetInfos = [incRetInfos];
            }
            incRetInfos.forEach(function(incRetInfo) {
                let finishDir = incRetInfo.finishDir
                let baseName = path.basename(finishDir);
                let toDir = path.join(backupdir, "incfinish");
                mkdirp.sync(toDir);
                let cmd_line = `cp  ${finishDir}/local/oplog.rs.bson ${toDir}/oplog.rs_${baseName}.bson`;
                cmdExe(cmd_line).then(function() {}).catch(function(err) {
                    console.log(err, err.stack)
                })
            })
        }
        return;
    } catch (err) {
        console.log(err, err.stack);
    }
}

backup({ backupdir: path.join(program.backupdir, program.db), db: program.db }).then(function() {
    console.log("backup ok")
}).catch(function(err) {
    console.log("backup fail", err)
});
//await backup({ backupdir: path.join(__dirname, "antiplagiarism"), db: "antiplagiarism" });
//await backup({ backupdir: path.join(__dirname, "fhh_oauth2"), db: "fhh_oauth2" });