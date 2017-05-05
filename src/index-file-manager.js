const fileManager = require('./file-manager')
const { INDEX_RECORD_SIZE } = require('./consts')

const indexFileManager = (chunkNumber, recordsPerFile, indexFile) => {
  const file = fileManager(indexFile)
  const readRecord = (localIndex) =>
    file.read(localIndex * INDEX_RECORD_SIZE, INDEX_RECORD_SIZE)
    .then((buffer) => ({
      offset: buffer.readUIntLE(0, 8), // unsigned-long (8 bytes)
      length: buffer.readUIntLE(8, 4), // unsigned-int (4 bytes)
      timestamp: buffer.readUIntLE(12, 8), // unsigned-long (8 bytes)
      localIndex: buffer.readUIntLE(20, 4), // unsigned-int (4 bytes)
      globalIndex: buffer.readUIntLE(24, 8) // unsigned-long (8 bytes)
    }))
  const writeRecord = (indexRecord) => {
    const timestamp = new Date().getTime()
    const localIndex = file.getSize() / INDEX_RECORD_SIZE
    const globalIndex = chunkNumber * recordsPerFile + localIndex
    const buffer = new Buffer(INDEX_RECORD_SIZE)
    buffer.writeUIntLE(indexRecord.offset, 0, 8)
    buffer.writeUIntLE(indexRecord.length, 8, 4)
    buffer.writeUIntLE(timestamp, 12, 8)
    buffer.writeUIntLE(localIndex, 20, 4)
    buffer.writeUIntLE(globalIndex, 24, 8)

    return file.append(buffer)
      .then(() => ({
        offset: indexRecord.offset,
        length: indexRecord.length,
        timestamp,
        localIndex,
        globalIndex
      }))
  }
  return {
    readRecord,
    writeRecord,
    close: file.close
  }
}
module.exports = indexFileManager
