const getConfig = require('microservice-config')
const { createTopicManager } = require('./src/topic-manager')
const { createEventBus } = require('./src/event-bus')
const { createApiServer } = require('./src/api-server')

const config = getConfig({
  data: './data',
  'http-host': '0.0.0.0',
  'http-port': 3000,
  'advertised-host': '',
  'advertised-port': 9190,
  'cluster-host': '0.0.0.0',
  'cluster-port': 9190
})

if (config.h || config['?'] || config.help) {
  console.log(`
      Usage: server.js <options>

      --data <path> [required]

      API configuration:
      --http-host <host>
      --http-port <port>

      Cluster configuration:
      --advertised-host <host> [required]
      --advertised-port <port>
      --cluster-host <host>
      --cluster-port <port>
    `).trim().split('\n').map((s) => s.trim()).join('\n')
  process.exit(0)
}

const eventBus = createEventBus()
const topicManager = createTopicManager(config, eventBus)
const apiServer = createApiServer(config, topicManager, eventBus)

process.on('SIGINT', () => {
  apiServer.close()
})
