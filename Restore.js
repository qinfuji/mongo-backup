/**
 * mongo 数据迁移
 */

var program = require('commander');
var path = require("path");
var fs = require("fs");

program
    .version('0.0.1')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--mode [value]', 'full|inc', '')
program.parse(process.argv);


if (!(program.backupdir && program.uri && program.mode)) {
    program.help()
    return;
}


let BackupDB = require("./ReplicaSetDB")
let backupDB = new BackupDB(program.uri);

if (program.mode == 'full') {
    //读取全量备份目录下所有备份目录
    //文件少的先读取
    //先restore的目录不处理索引
    //然后处理增量文件, 先合并文件
    //让后再restore oplog
    let fullbackupDir1 = path.join(program.backupdir, "full/fhhshard1")
    let fullbackupDir2 = path.join(program.backupdir, "full/fhhshard2")
    await backupDB.fullRestore({
        dir: fullbackupDir1, //原始的处理
        noIndexRestore: false, //是否重新处理索引
        drop: true,
    });

    await backupDB.fullRestore({
        dir: fullbackupDir2, //原始的处理
        noIndexRestore: true, //是否重新处理索引
        drop: false,
    });
}

//执行增量处理