const fs = require('fs')

const fileManager = (file) => {
  // Promise that is opening the file, or null if the file is closed.
  var opener = null
  var timer = null
  var size = file.size
  var writeBuffer = []
  const open = () => {
    if (timer) clearTimeout(timer)
    timer = setTimeout(close, 1000)
    return opener || (opener = new Promise((resolve, reject) => {
      // console.info(`Opening ${file.fullPath}`)
      fs.open(file.fullPath, 'a+', (err, fd) => err ? reject(err) : resolve(fd))
    }))
  }

  const close = () => {
    if (timer) {
      clearTimeout(timer)
      timer = null
    }
    if (opener) {
      const old = opener
      opener = null
      return old.then((fd) => new Promise((resolve, reject) => {
        // console.info(`Closing ${file.fullPath}`)
        fs.close(fd, (err) => err ? reject(err) : resolve())
      }))
    } else {
      return Promise.resolve()
    }
  }

  const read = (offset, length) => open().then((fd) => new Promise((resolve, reject) => {
    fs.read(
      fd,
      new Buffer(length),
      0, length, offset,
      (err, bytesRead, buffer) =>
        bytesRead !== length
        ? reject(`Expected ${length}, got ${bytesRead} bytes read.`)
        : err
        ? reject(err)
        : resolve(buffer)
    )
  }))

  var writing = false
  const evalWriteBuffer = () => {
    if (!writing) {
      if (writeBuffer.length) {
        writing = true
        const {resolve, reject, buffer} = writeBuffer.shift()
        open().then((fd) =>
          fs.write(fd, buffer, 0, buffer.length, size, (err, written) => {
            if (written !== buffer.length) {
              reject(`Expected ${buffer.length}, got ${written} bytes written.`)
            } else if (err) {
              reject(err)
            } else {
              resolve({
                offset: size,
                length: written
              })
              writing = false
              size += written
              evalWriteBuffer()
            }
          })
        ).catch(reject)
      }
    }
  }

  const append = (buffer) => new Promise((resolve, reject) => {
    writeBuffer.push({resolve, reject, buffer})
    evalWriteBuffer()
  })

  const getSize = () => size

  return {
    append,
    read,
    close,
    getSize
  }
}

module.exports = fileManager
