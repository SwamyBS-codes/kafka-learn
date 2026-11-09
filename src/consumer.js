const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'infinte' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'infinte', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString()
      })
    }
  })
}

run().catch((error) => {
  console.error('Consumer error:', error)
  process.exit(1)
})