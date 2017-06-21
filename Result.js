function ok(message) {
    return {
        success: true,
        msg: message
    }
}

function fail(message) {
    return {
        success: false,
        msg: message
    }
}

module.exports = {
    ok,
    fail
}