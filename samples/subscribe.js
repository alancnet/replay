const request = require('request')

const topicUrl = process.argv[2]
if (!topicUrl) {
  console.error('Usage: subscribe.js http://localhost:3000/topics/<my-topic-name>')
  process.exit(1)
}

const poll = (url) => {
  request(url, (err, res, body) => {
    if (err) return console.error(err)
    const doc = JSON.parse(body)
    doc.messages.forEach((message) => {
      console.log(`${new Date(message.timestamp)}: ${message.value}`)
    })
    poll(doc.nextUrl)
  })
}

poll(`${topicUrl}/subscribe?offset=0&encoding=utf8`)
