// const fs = require('fs')
//
// fs.open('./test.txt', 'a+')
// .then((file) => file.write('hello', 0, 3))
// .then((file) => file.close())

const topic = require('./src/topic')

topic('findme2').then((topic) => Promise.all(
  Array(2).fill(0).map((x, i) => i)
    .map((i) => topic.write(new Buffer(`Hello World - ${i} - ${new Date()}`)))
  )
).catch(console.error)
.then(topic.close)

// topic('findme').then((topic) => {
//   topic.read(9999, 2).subscribe((x) => {
//     x.value = x.value.toString()
//     console.log(x)
//   }, console.error, () => {
//     topic.close()
//   })
// })
