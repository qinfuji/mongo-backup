/**
 * mongo 数据迁移
 */

const program = require('commander');
const path = require("path");
const fs = require("fs");
const fileUtils = require("./fileUtils");
const oplog = require("./Oplog");
const { cmdExe } = require("./utils");
const mkdirp = require('mkdirp');
const moment = require('moment');
program
    .version('0.0.1')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--mode [value]', 'full|inc', '')
    .option('--db [value]', '恢复的数据库', '')
program.parse(process.argv);

console.log("------------------------------------------------------------------------");
console.log("------------------------------------------------------------------------");

if (!(program.backupdir && program.uri && program.mode && program.db)) {
    program.help()
    return;
}


let BackupDB = require("./ReplicaSetDB")
let backupDB = new BackupDB(program.uri);

async function restore({ backdir, restoreDB }) {

    let startTime = new Date().getTime();
    if (program.mode == 'full') {
        //读取全量备份目录下所有备份目录
        //文件少的先读取
        //先restore的目录不处理索引
        //然后处理增量文件, 先合并文件
        //让后再restore oplog
        let fullDir = path.join(backdir, "full");
        mkdirp.sync(fullDir);
        let ret = cmdExe(`mv ${fullDir} ${fullDir}_${moment().format("YYYYMMDDhhmmss")}`);
        mkdirp.sync(outputDir)
        let fullbackupDir = path.join(backdir, "full")
        let dirs = await fileUtils.findChildDir(fullbackupDir);

        if (!dirs || !dirs.length) {
            throw new Error("没有需要处理的数据")
        }
        let backupDirs = [];
        dirs.forEach(function(dir) {
            let files = fileUtils.getAllFiles(dir, true);
            backupDirs.push({ dir: dir, length: files.length });
        })

        backupDirs = backupDirs.sort(function(a, b) {
            return a.length < b.length;
        })
        for (let i = 0; i < backupDirs.length; i++) {
            await backupDB.fullRestore({
                backup_dir: path.join(backupDirs[i].dir, restoreDB), //原始的处理
                noIndexRestore: i != 0, //第一次restore重建索引，TODO 这里本应该通过数据库的主shard来判断是否新建索引？
                drop: i == 0, //只有第一次才需要drop数据库
                db: restoreDB
            });
        }
        let msg = `ReplicaSetDB fullrestore finish , ${(new Date().getTime()-startTime)/1000}`;
    } else if (program.mode == 'inc') {

        //获取数据库的所有增量文件
        let incBackupDir = path.join(backdir, "incfinish");
        let files = fileUtils.getAllFiles(incBackupDir, true);
        //合并并排序所有增量文件
        let outputDir = path.join(incBackupDir, "temp");
        mkdirp.sync(outputDir)
        let outputFile = path.join(outputDir, "oplog.bson");
        oplog.merge(outputFile, files); //合并所有oplog文件
        oplog.vailde(outputFile); //验证合并的文件是否有问题
        //恢复增量文件
        console.log(`restore merge oplog finish . ${(new Date().getTime()-startTime)/1000}`)
        backupDB.incRestore({
            backup_dir: outputDir
        }).then(function() {
            console.log(`increstore finish . ${(new Date().getTime()-startTime)/1000}`)
                //移动数据
            cmdExe(`rm -rf ${outputDir}`).then(function() {
                console.log(`rm -rf ${outputDir} ok`)
            }).catch(function(err) {
                console.log(err, err.stack)
            })
            let mvtodir = `${backdir}/oplog.restored`;
            mkdirp.sync(mvtodir)
            let cmd_line = `mv ${files.join(" ")} ${mvtodir}`
            cmdExe(cmd_line).then(function() {}).catch(function(err) {
                console.log(err, err.stack)
            })
        });
    }
}

restore({
    backdir: program.backupdir,
    restoreDB: program.db
}).catch(function(err) {
    console.log(err, err.stack)
})