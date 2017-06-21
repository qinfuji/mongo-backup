/**
 * mongo 数据迁移
 */

var program = require('commander');

program
    .version('0.0.1')
    .option('--deploy [value]', '目标数据库模式，replSet|sharding', '')
    .option('--backupdir [value]', '备份目录', '')
    .option('--uri [value]', 'mongodb链接字符串', '')
    .option('--action [value]', '操作命令，dump|restore', '')
    .option('--mode [value]', 'full|inc', '')
    .option('--db [value]', '备份数据库', '')
program.parse(process.argv);


if (!(program.backupdir && program.deploy && program.backupdir && program.uri && program.action && program.mode)) {
    program.help()
    return;
}

if (program.deploy == 'replSet') { //复制集

    let BackupDB = null;
    if (program.deploy == 'replSet') {
        BackupDB = require("./ReplicaSetDB")
    } else if (program.deploy == 'sharding') {
        BackupDB = require("./ShardingDB")
    }
    let backupDB = new BackupDB(program.uri);
    let backupInfo = {
        backup_dir: program.backupdir,
        db: program.db
    }
    if (program.action == 'dump' && program.mode == 'full') {
        await backupDB.fullbackup(backupInfo);
    } else if (program.action == 'dump' && program.mode == 'inc') {
        await backupDB.incbackup(backupInfo);
    } else if (program.action == 'restore' && program.mode == 'full') {

    } else if (program.action == 'restore' && program.mode == 'inc') {

    }
}