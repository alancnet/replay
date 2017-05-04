// const fs = require('fs')
//
// fs.open('./test.txt', 'a+')
// .then((file) => file.write('hello', 0, 3))
// .then((file) => file.close())

const topic = require('./src/topic')

// topic('findme').then((topic) => Promise.all(
//   Array(20000).fill(0).map((x, i) => i)
//     .map((i) => topic.write(new Buffer(`Hello World - ${i}`)))
//   )
// ).catch(console.error)
// .then(topic.close)

// topic('findme').then((topic) => {
//   topic.read(0, 10).subscribe((x) => {
//     console.log(x.value.toString())
//   }, console.error, () => {
//     topic.close()
//   })
// })
