if (typeof setImmediate === 'function') {
    module.exports = setImmediate;
} else {
    module.exports = process.nextTick;
}