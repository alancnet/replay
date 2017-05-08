const {createTopic} = require('./topic')

const createTopicManager = (config, eventBus) => {
  const topics = {}
  const getTopic = (topicName) =>
    topics[topicName] || (topics[topicName] = createTopic(topicName, {
      dataRoot: config.data
    }, eventBus))

  return {
    getTopic
  }
}

module.exports = {createTopicManager}
