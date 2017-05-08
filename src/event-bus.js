const { Subject } = require('rxjs')
const createEventBus = () => {
  const topics = {}
  const getTopic = (name) => topics[name] || (topics[name] = new Subject())
  const publish = (name, message) => getTopic(name).next(message)
  const subscribe = (name, observer) => getTopic(name).subscribe(observer)

  return {
    getTopic,
    publish,
    subscribe
  }
}

module.exports = {createEventBus}
