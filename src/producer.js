const { Kafka, Partitioners } = require('kafkajs')
const readline = require('readline')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  idempotent: true,
  maxInFlightRequests: 5,
  retry: {
    retries: Number.MAX_SAFE_INTEGER
  }
})

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

const askForMessage = () => {
  rl.question('Enter message to send (or type exit): ', async (message) => {
    if (message.trim().toLowerCase() === 'exit') {
      await producer.disconnect()
      rl.close()
      return
    }

    if (message.trim()) {
      try {
        await producer.send({
          topic: 'infinte',
          acks: -1,
          messages: [{ key: 'producer-1', value: message }]
        })
        console.log('Message sent:', message)
      } catch (error) {
        console.error('Send failed:', error)
      }
    }

    askForMessage()
  })
}

const run = async () => {
  await producer.connect()
  console.log('Interactive producer started. Type a message and press Enter.')
  askForMessage()
}

run().catch((error) => {
  console.error('Producer error:', error)
  process.exit(1)
})