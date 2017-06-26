const fs = require("fs");
const path = require("path");


function findChildDir(dirPath) {
    if (!dirPath) throw new Error("dirPath not defined");
    try {
        let files = fs.readdirSync(dirPath)
        let childDirs = [];
        files.forEach(function(file) {
            file = path.resolve(dirPath, file);
            let stat = fs.statSync(file);
            if (stat && stat.isDirectory()) {
                console.log(file)
                childDirs.push(file)
            }
        })
        return childDirs;
    } catch (err) {
        throw err
    }
}

module.exports.findChildDir = findChildDir;

/**
 * 获取目录先的文件数量
 * @param dirPath 
 * @param filterDir 是否过滤目录
 * @param retFiles  返回文件
 */
function getAllFiles(dirPath, filterDir, retfiles) {
    if (!retfiles) {
        retfiles = [];
    }
    if (!dirPath) throw new Error("dirPath not defined");
    try {
        let files = fs.readdirSync(dirPath)
        let childDirs = [];
        files.forEach(function(file) {
            file = path.resolve(dirPath, file);
            let stat = fs.statSync(file);
            if (stat && (stat.isFile() || (stat.isDirectory() && !filterDir))) {
                retfiles.push(file)
            }
            if (stat && stat.isDirectory()) {
                getAllFiles(file, filterDir, retfiles);
            }
        })
        return retfiles;
    } catch (err) {
        throw err
    }
}

module.exports.getAllFiles = getAllFiles;




// getAllFiles("/Users/qinfuji/mongodbback/fhh_oauth2").then(function(result) {
//     console.log(result);
// }).catch(function(err) {
//     console.log(err);
// });

// let v = getAllFiles("/Users/qinfuji/mongodbback/fhh_oauth2")
// console.log(v);