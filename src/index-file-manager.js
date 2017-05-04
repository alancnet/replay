const fileManager = require('./file-manager')
const { INDEX_RECORD_SIZE } = require('./consts')

const indexFileManager = (indexFile) => {
  const file = fileManager(indexFile)
  const readRecord = (localIndex) =>
    file.read(localIndex * INDEX_RECORD_SIZE, INDEX_RECORD_SIZE)
    .then((buffer) => ({
      offset: buffer.readInt32LE(0),
      length: buffer.readInt32LE(4)
    }))
  const writeRecord = (indexRecord) => {
    const buffer = new Buffer(INDEX_RECORD_SIZE)
    buffer.writeInt32LE(indexRecord.offset, 0)
    buffer.writeInt32LE(indexRecord.length, 4)
    return file.append(buffer)
  }
  return {
    readRecord,
    writeRecord,
    close: file.close
  }
}
module.exports = indexFileManager
