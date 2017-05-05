const express = require('express')
const expressRest = require('express-rest')
const getConfig = require('microservice-config')
const fs = require('fs')
const { normalize } = require('path')
const topic = require('./src/topic')
const eventBus = require('./src/event-bus')()
const { Subject } = require('rxjs')
const valueEncoder = require('./src/value-encoder')

const config = getConfig({
  data: './data',
  port: 3000
})

const topics = {}
const getTopic = (topicName) =>
  topics[topicName] || (topics[topicName] = topic(topicName, {
    dataRoot: config.data
  }, eventBus))

const app = express()
const rest = expressRest(app)

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
  getTopic(req.params.topicName).then((topic) => rest.ok({
    length: topic.getLength(),
    replayUrl: `${baseUrl(req)}/topic/${req.params.topicName}/subscribe?offset=0&size=20&encoding=base64&timeout=5000`
  }))
)

rest.get('/topics/:topicName/subscribe', (req, rest) => {
  const offset = +(req.query.offset || 0)
  const size = +(req.query.size || 20)
  const timeout = +(req.query.timeout || 5000)
  const encoding = req.query.encoding || 'base64'

  return Promise.all([
    getTopic(req.params.topicName),
    valueEncoder(encoding)
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
    const entireBuffer = Buffer.concat(incoming)

    var contentType = req.headers['content-type']
    var body = contentType === 'application/json'
      ? JSON.parse(entireBuffer.toString())
      : {
        encoding: 'buffer',
        messages: [{
          value: entireBuffer
        }]
      }

    if (body && body.messages) {
      Promise.all([
        getTopic(req.params.topicName),
        valueEncoder(body.encoding)
      ]).then(([topic, valueEncoder]) => {
        const messages = body.messages.map((message) => Object.assign({}, message, {
          value: valueEncoder.decode(message.value)
        }))
        return Promise.all(messages.map((message) => topic.write(message.value)))
          .then((messages) => {
            res.set('Content-Type', 'application/json')
            const json = JSON.stringify({messageOffsets: messages})
            res.send(json)
            res.end()
          })
      })
        .catch((err) => {
          res.status(500).send(err.message)
          res.end()
        })
    } else {
      res.status(415).send(`Unsupported media type: ${contentType}`)
      res.end()
    }
  })
})

app.listen(config.port)
