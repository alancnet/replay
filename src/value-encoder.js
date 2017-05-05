const { encode, decode } = require('base64-arraybuffer')

const valueEncoder = (type) => new Promise((resolve, reject) =>
  type === 'buffer'
  ? resolve(bufferEncoder)
  : type === 'base64'
  ? resolve(base64Encoder)
  : type === 'utf8'
  ? resolve(stringEncoder)
  : type === 'json'
  ? resolve(jsonEncoder)
  : reject('Supported encodings are: buffer, base64, utf8, json')
)

const bufferEncoder = {
  encode: (buffer) => new Buffer(buffer),
  decode: (buffer) => new Buffer(buffer)
}

const base64Encoder = {
  encode: (buffer) => encode(buffer.data || buffer),
  decode: (base64String) => new Buffer(decode(base64String))
}

const stringEncoder = {
  encode: (buffer) => buffer.toString(),
  decode: (string) => new Buffer(string)
}

const jsonEncoder = {
  encode: (buffer) => JSON.parse(buffer.toString()),
  decode: (obj) => JSON.stringify(obj)
}

module.exports = valueEncoder
