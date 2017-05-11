const express = require('express')
const expressRest = require('express-rest')
const fs = require('fs')
const { normalize } = require('path')
const { Subject, ReplaySubject } = require('rxjs')
const { getValueEncoder } = require('./value-encoder')
const expressWs = require('express-ws')

const createApiServer = (config, topicManager, eventBus) => {
  const app = express()
  const rest = expressRest(app)
  expressWs(app)

  app.use((req, res, next) => {
    req.url = req.protocol + '://' + req.get('host') + req.originalUrl
    console.log(req.method, req.url)
    next()
  })

  const globalTerminator = new Subject()

  process.on('SIGINT', () => globalTerminator.next())

  const baseUrl = (req) => `${req.protocol}://${req.get('host')}`

  rest.get('/', (req, rest) => rest.ok({
    topicsUrl: `${baseUrl(req)}/topics`
  }))

  rest.get('/topics', (req, rest) =>
    fs.readdir(config.data, (err, files) =>
      err ? rest.internalServerError(err.message)
      : Promise.all(
        files.map((file) => new Promise((resolve, reject) => {
          fs.stat(normalize(`${config.data}/${file}`), (err, stat) =>
            err ? reject(err)
            : resolve(stat.isDirectory() ? file : null)
          )
        }))
      )
        .then((dirs) => dirs.filter((x) => x).sort())
        .then((dirs) => rest.ok(dirs.map((dir) => ({
          name: dir,
          url: `${baseUrl(req)}/topics/${dir}`
        }))))
    )
  )

  rest.get('/topics/:topicName', (req, rest) =>
    topicManager.getTopic(req.params.topicName).then((topic) => rest.ok({
      length: topic.getLength(),
      replayUrl: `${baseUrl(req)}/topics/${req.params.topicName}/subscribe?offset=0&size=20&encoding=base64&timeout=5000`,
      pollUrl: `${baseUrl(req)}/topics/${req.params.topicName}/subscribe?offset=${topic.getLength()}&size=20&encoding=base64&timeout=5000`
    }))
  )

  rest.get('/topics/:topicName/subscribe', (req, rest) => {
    const offset = +(req.query.offset || 0)
    const size = +(req.query.size || 20)
    const timeout = +(req.query.timeout || 5000)
    const encoding = req.query.encoding || 'base64'

    if (offset < 0 || isNaN(offset)) return rest.badRequest('offset is invalid')
    if (size < 0 || isNaN(size)) return rest.badRequest('size')
    if (timeout < 0 || isNaN(timeout)) return rest.badRequest('timeout')

    return Promise.all([
      topicManager.getTopic(req.params.topicName),
      getValueEncoder(encoding)
    ]).then(([topic, valueEncoder]) => {
      const handleMessages = (messages) => {
        try {
          const newOffset = messages.length ? messages[messages.length - 1].offset + 1 : offset
          rest.ok({
            messages: messages.map((message) => Object.assign({}, message, {
              value: valueEncoder.encode(message.value)
            })),
            nextUrl: `${baseUrl(req)}/topics/${req.params.topicName}/subscribe?offset=${newOffset}&size=${size}&encoding=${encoding}&timeout=${timeout}`
          })
        } catch (ex) { rest.internalServerError(ex.message) }
      }

      if (offset >= topic.getLength()) {
        // Stream future results
        const terminator = new Subject()
        setTimeout(() => terminator.next(), timeout)
        // The subscription here is guaranteed to occur before new messages are
        // sent. Don't mess this up!
        const subscription = eventBus.getTopic(req.params.topicName)
          .takeUntil(terminator)
          .takeUntil(globalTerminator)
          .do(() => setImmediate(() => terminator.next(0)))
          .toArray()
          .subscribe(handleMessages, rest.internalServerError)

        req.on('close', () => subscription.unsubscribe())
      } else {
        topic.read(offset, Math.max(0, Math.min(size, topic.getLength() - offset)))
          .toArray()
          .subscribe(handleMessages, rest.internalServerError)
      }
    })
      .catch((err) => rest.internalServerError(err.message))
  })

  app.post('/topics/:topicName/publish', (req, res, next) => {
    const incoming = []
    req.on('data', (data) => incoming.push(data))
    req.on('end', () => {
      const contentLengths = (
        req.headers['x-content-lengths'] ||
        req.headers['content-length'] ||
        '0'
      ).split(',').map((s) => parseInt(s))

      const entireBuffer = Buffer.concat(incoming)

      var contentType = req.headers['content-type']

      topicManager.getTopic(req.params.topicName).then((topic) =>
        Promise.all(contentLengths.map((contentLength, i) => {
          const subBuffer = entireBuffer.slice(
            contentLengths.slice(0, i).reduce((a, b) => a + b, 0),
            contentLengths.slice(0, i + 1).reduce((a, b) => a + b, 0)
          )
          var body = contentType === 'application/json'
            ? JSON.parse(subBuffer.toString())
            : {
              encoding: 'buffer',
              messages: [{
                value: subBuffer
              }]
            }

          if (body && body.messages) {
            return getValueEncoder(body.encoding).then((valueEncoder) => {
              const messages = body.messages.map((message) => Object.assign({}, message, {
                value: valueEncoder.decode(message.value)
              }))
              return Promise.all(messages.map((message) =>
                topic.write(message.value)
              ))
            })
          } else {
            throw Object.assign(new Error(`Unsupported media type: ${contentType}`), {
              status: 415
            })
          }
        })) // Map
      )
        .then((messageOffsetArrays) => {
          const messageOffsets = messageOffsetArrays.reduce((a, b) => a.concat(b), [])
          res.set('Content-Type', 'application/json')
          const json = JSON.stringify({messageOffsets})
          res.send(json)
          res.end()
        })
        .catch((err) => {
          res.status(err.status || 500).send(err.message)
          res.end()
        })
    })
  })

  app.ws('/', (ws, req) => {
    var aborted = false
    const messages = new Subject()
    const socketTerminator = new ReplaySubject()
    ws.on('message', (message) => {
      messages.next(message)
    })
    const handleError = (err) => {
      if (!aborted) {
        aborted = true
        ws.send(JSON.stringify({
          type: 'error',
          message: err.message
        }))
        ws.close()
      }
      socketTerminator.next()
    }

    const incoming = messages.map(JSON.parse).share()
    const outgoing = new Subject()
    outgoing.map(JSON.stringify)
      .subscribe((message) => !aborted && ws.send(message), handleError)
    // Subscriptions
    incoming
      .filter((m) => m.type === 'subscribe')
      .subscribe((m) => {
        const topicTerminator = new ReplaySubject()
        incoming
          .filter((x) => x.type === 'unsubscribe' && x.topic === m.topic)
          .take(1)
          .subscribe(topicTerminator)
        if (!m.topic) throw new Error('subscribe requires topic')
        return Promise.all([
          topicManager.getTopic(m.topic),
          getValueEncoder(m.encoding || 'base64')
        ]).then(([topic, valueEncoder]) => {
          const handleMessage = (message) => {
            outgoing.next(Object.assign({}, message, {
              value: valueEncoder.encode(message.value)
            }))
          }
          const read = (offset) => {
            if (offset >= topic.getLength()) {
              // Stream future results
              // The subscription here is guaranteed to occur before new messages are
              // sent. Don't mess this up!
              eventBus.getTopic(m.topic)
                .takeUntil(topicTerminator)
                .takeUntil(globalTerminator)
                .map((message) => Object.assign({
                  type: 'message',
                  topic: m.topic
                }, message))
                .subscribe(handleMessage, handleError)
            } else {
              var newOffset = offset - 1
              topic.read(offset, Math.max(0, topic.getLength() - offset))
                .takeUntil(topicTerminator)
                .takeUntil(globalTerminator)
                .do((message) => { newOffset = message.offset })
                .map((message) => Object.assign({
                  type: 'message',
                  topic: m.topic
                }, message))
                .subscribe(handleMessage, handleError, () => {
                  if (newOffset > offset) read(newOffset + 1)
                })
            }
          }
          read(m.offset || 0)
        })
      }, handleError)

    // Publishes
    incoming
      .filter((m) => m.type === 'publish')
      .groupBy((m) => `${m.topic}/${m.encoding}`)
      .subscribe((group) => {
        const [topicName, encoding] = group.key.split('/')
        const groupShare = group.share()
        const temp = new ReplaySubject()
        const tempSub = groupShare.subscribe(temp)
        Promise.all([
          topicManager.getTopic(topicName),
          getValueEncoder(encoding)
        ]).then(([topic, valueEncoder]) => {
          temp.merge(groupShare).subscribe((m) => {
            const buffer = valueEncoder.decode(m.value)
            topic.write(buffer)
              .then((offset) => outgoing.next({
                type: 'published',
                topicName,
                offset
              }))
          })
          tempSub.unsubscribe()
          temp.complete()
        })
      }, handleError)

    const close = () => {
      socketTerminator.next()
      ws.close()
      killSub.unsubscribe()
    }
    const killSub = globalTerminator.subscribe(close)
    ws.on('close', () => {
      aborted = true
      close()
    })
    ws.on('error', (err) => {
      aborted = true
      console.error(err)
      close()
    })
  })

  const server = app.listen(config['http-port'], config['http-host'])

  return {
    close: () => {
      globalTerminator.next(0)
      server.close()
    }
  }
}
module.exports = {createApiServer}
