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

program
    .version('0.0.1')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--mode [value]', 'full|inc', '')
    .option('--db [value]', '恢复的数据库', '')
program.parse(process.argv);


if (!(program.backupdir && program.uri && program.mode && program.db)) {
    program.help()
    return;
}


let BackupDB = require("./ReplicaSetDB")
let backupDB = new BackupDB(program.uri);

async function restore({ backdir, restoreDB }) {
    if (program.mode == 'full') {
        //读取全量备份目录下所有备份目录
        //文件少的先读取
        //先restore的目录不处理索引
        //然后处理增量文件, 先合并文件
        //让后再restore oplog
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
            return a.length > b.length;
        })
        for (let i = 0; i < backupDirs.length; i++) {
            let noIndexRestore = backupDirs.length == 1 ? false : (i < backupDirs.length - 1)
            await backupDB.fullRestore({
                backup_dir: path.join(backupDirs[i].dir, restoreDB), //原始的处理
                noIndexRestore: noIndexRestore, //是否重新处理索引
                drop: i == 0, //只有第一次才需要drop数据库
                db: restoreDB
            });
        }
    }

    //获取数据库的所有增量文件
    let incBackupDir = path.join(backdir, "incfinish");
    let files = fileUtils.getAllFiles(incBackupDir, true);
    //合并并排序所有增量文件
    let outputDir = path.join(incBackupDir, "temp");
    mkdirp.sync(outputDir)
    let outputFile = path.join(outputDir, "oplog.bson");
    oplog.merge(outputFile, files);
    oplog.vailde(outputFile);
    //恢复增量文件
    console.log("restore oplog dir:", outputDir)
    backupDB.incRestore({
        backup_dir: outputDir
    }).then(function() {
        //移动数据
        let mvtodir = `${backdir}/oplog.restored`;
        mkdirp.sync(mvtodir)
        let cmd_line = `mv ${files.join(" ")} ${mvtodir}`
        cmdExe(cmd_line).then(function() {}).catch(function(err) {
            console.log(err, err.stack)
        })
    });
}

restore({
    backdir: program.backupdir,
    restoreDB: program.db
}).catch(function(err) {
    console.log(err, err.stack)
})