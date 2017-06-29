const { exec } = require('child_process');
const Result = require("./Result");

module.exports.getUriInfo = function(url) {
    let ret = /mongodb:\/\/(?:(.*):(.*)@)?([^?]*)(?:\?replicaSet\=(.*))?/gi.exec(url)
    return {
        username: ret[1],
        password: ret[2],
        ips: ret[3].split(','),
        replSetName: ret[4]
    }
}

module.exports.cmdExe = async function(cmd) {
    console.log(cmd)
    return new Promise((resolve, reject) => {
        // exec(cmd, { maxBuffer: 5000 * 1024 }, (error, stdout, stderr) => {
        //     console.log(`stdout: ${stdout}`);
        //     console.log(`stderr: ${stderr}`);
        //     if (error) {
        //         console.log(`${cmd}  error: ${ error }`)
        //         reject(Result.fail(`${cmd}  error: ${ error }`))
        //         return;
        //     }
        //     console.log(`${cmd} ok`)
        //     resolve(Result.ok(`${cmd} ok`))
        // });
        resolve(Result.ok(`${cmd} ok`))
    })
}