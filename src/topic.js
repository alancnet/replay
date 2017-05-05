const _ = require('lodash')
const filenamify = require('filenamify')
const { normalize } = require('path')
const mkdirp = require('mkdirp')
const fs = require('fs')
const { Observable } = require('rxjs')
const { INDEX_RECORD_SIZE } = require('./consts')
const logFileManager = require('./log-file-manager')
const indexFileManager = require('./index-file-manager')

const topic = (name, _config, eventBus) => new Promise((resolve, reject) => {
  const config = _.defaults(_config, {
    dataRoot: './data',
    recordsPerFile: 10000
  })

  var length

  const path = normalize(`${config.dataRoot}/${filenamify(name)}`)

  const managers = {}
  const getManager = (name, which) => new Promise((resolve, reject) => {
    if (managers[name]) {
      resolve(managers[name])
    } else {
      const fullPath = normalize(`${path}/${name}`)
      fs.stat(fullPath, (err, stats) => {
        const size = err ? 0 : stats.size
        const manager = which({name, fullPath, size})
        managers[name] = manager
        resolve(manager)
      })
    }
  })

  const getIndexFileManagerByFilename = (name, chunkNumber) =>
    getManager(name, indexFileManager.bind(null, chunkNumber, config.recordsPerFile))
  const getLogFileManagerByFilename = (name) =>
    getManager(name, logFileManager)

  const getFiles = () => new Promise((resolve, reject) => {
    fs.readdir(path, (err, files) => {
      if (err) return reject(err)
      resolve(Promise.all(
        files.map((name) => new Promise((resolve, reject) => {
          const fullPath = normalize(`${path}/${name}`)
          fs.stat(fullPath, (err, stats) => {
            if (err) return reject(err)
            resolve({
              fullPath,
              name,
              size: stats.size
            })
          })
        }))
      ))
    })
  })

  const indexR = /^index-(\d*)\.bin$/
  const getIndexFiles = () => getFiles()
    .then((files) => files.map((file) => {
      const r = indexR.exec(file.name)
      return r && Object.assign({}, file, {
        index: +r[1]
      })
    }).filter((x) => x))

  const computeLength = () => getIndexFiles().then((indexes) => {
    // Verify all lower indexes are full
    indexes.slice(0, -1).forEach((index) => {
      if (index.size !== INDEX_RECORD_SIZE * config.recordsPerFile) {
        throw new Error(`Lower index is incorrect size: ${index.name} -> ${index.size}`)
      }
    })
    // Verify last index increments
    const lastIndex = indexes[indexes.length - 1] || {index: 0, size: 0}
    if (lastIndex.size % INDEX_RECORD_SIZE !== 0) {
      throw new Error(`Last index is incorrect size: ${lastIndex.name} -> ${lastIndex.size}`)
    }
    return (lastIndex.index * config.recordsPerFile) + (lastIndex.size / INDEX_RECORD_SIZE)
  })

  const getChunkNumber = (offset) => Math.floor(offset / config.recordsPerFile)

  const getManagersForChunk = (chunkNumber) => {
    const indexFilename = `index-${chunkNumber}.bin`
    const logFilename = `log-${chunkNumber}.log`
    return Promise.all([
      getIndexFileManagerByFilename(indexFilename, chunkNumber),
      getLogFileManagerByFilename(logFilename)
    ])
  }

  const getManagersForOffset = (offset) => getManagersForChunk(getChunkNumber(offset))

  var writeBuffer = []
  var writing = false

  const evalWriteBuffer = () => {
    if (!writing) {
      if (writeBuffer.length) {
        writing = true
        const {resolve, reject, buffer} = writeBuffer.shift()
        getManagersForOffset(length)
        .then(([indexFile, logFile]) =>
          logFile.writeLog(buffer)
            .then(indexFile.writeRecord)
            .then((indexRecord) => {
              writing = false
              resolve(length++)
              eventBus.publish(name, {
                timestamp: indexRecord.timestamp,
                offset: indexRecord.globalIndex,
                value: buffer
              })
              evalWriteBuffer()
            })
        )
        .catch(reject)
      }
    }
  }
  const write = (buffer) => new Promise((resolve, reject) => {
    writeBuffer.push({resolve, reject, buffer})
    evalWriteBuffer()
  })

  const read = (readOffset, readLength) => Observable.create((observer) => {
    if (readOffset + readLength > length) {
      observer.error('Out of range')
    } else {
      var chunk = -1
      var index = null
      var log = null
      var abort = false
      const go = (offset) => {
        if (abort || offset >= (readOffset + readLength)) {
          return observer.complete()
        }
        const newChunk = getChunkNumber(offset)
        if (chunk !== newChunk) {
          getManagersForChunk(newChunk)
            .then(([indexFileManager, logFileManager]) => {
              index = indexFileManager
              log = logFileManager
              chunk = newChunk
              go(offset)
            })
        } else {
          index.readRecord(offset % config.recordsPerFile)
            .then((record) =>
              log.readLog(record)
              .then((value) => observer.next({
                timestamp: record.timestamp,
                offset,
                value
              }))
              .then(() => go(offset + 1))

            )
            .catch((err) => observer.error(err))
        }
      }

      go(readOffset)

      return () => {
        abort = true
      }
    }
  })

  const close = () => Promise.all(
    _.values(managers)
      .map((manager) => manager.close())
  )

  const getLength = () => length

  mkdirp(path, (err) => {
    if (err) return reject(err)
    computeLength().then((l) => {
      length = l
    })
    .then(() => {
      resolve({
        read,
        write,
        close,
        getLength
      })
    })
    .catch(reject)
  })
})

module.exports = topic
