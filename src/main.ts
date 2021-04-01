import { Kafka, Message } from 'kafkajs';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
});

async function produceData(): Promise<void> {
  const producer = kafka.producer();
  await producer.connect();
  const messages: Message[] = [];
  for (let i = 0; i < 1000; i += 1) {
    messages.push({value: `message${i}`});
  }
  await producer.sendBatch({
    topicMessages: [
      {
        topic: 'kafka_test',
        messages,
      }
    ],
  })
  await producer.disconnect();
}

function newOffset(offset: string): string {
  return (BigInt(offset) + BigInt(1)).toString();
}

async function main(): Promise<void> {
  await produceData();
  const consumer = kafka.consumer({
    readUncommitted: false,
    groupId: 'test_group',
  });
  await consumer.connect();
  const producer = kafka.producer({
    transactionalId: 'test_transaction',
    maxInFlightRequests: 1,
    idempotent: true,
  });
  await producer.connect();
  await consumer.subscribe({ topic: 'kafka_test', fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, heartbeat, resolveOffset }) => {
      const interval = setInterval(() => heartbeat(), 1000);
      for (const message of batch.messages) {
        try {
          const transaction = await producer.transaction();
          await transaction.sendOffsets({
            topics: [
              {
                topic: batch.topic,
                partitions: [
                  {
                    partition: batch.partition,
                    offset: newOffset(message.offset),
                  }
                ],
              },
            ],
            consumerGroupId: 'test_group',
          });
          console.log('Consuming message: ', message.offset);
          await new Promise((res) => setTimeout(res, 100));
          await transaction.commit();
          // await new Promise((res) => setTimeout(res, 100));
          resolveOffset(message.offset);
        } catch (e) {
          if (e.code === 51) {
            console.log('----------------');
            console.log('We got a problem');
            console.log('----------------');
            process.exit(1);
          }
        }
      }
      clearInterval(interval);
    },
  });
}

main().catch(console.error.bind(console));
