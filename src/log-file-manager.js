const fileManager = require('./file-manager')

const logFileManager = (logFile) => {
  const file = fileManager(logFile)

  const readLog = (indexRecord) =>
    file.read(indexRecord.offset, indexRecord.length)

  const writeLog = (buffer) => Promise.all([
    file.append(buffer),
    file.append(new Buffer('\n')) // So it can be viewed as a text file
  ]).then(([ret]) => ret)

  return {
    readLog,
    writeLog,
    close: file.close
  }
}

module.exports = logFileManager
