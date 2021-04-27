import { Admin, Consumer, Kafka } from 'kafkajs';
import { ConsumerMetaStorage } from './consumer-meta-storage';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
});

const GROUP_ID = 'test_group';

function waitForGroupJoin(consumer: Consumer): Promise<void> {
  return new Promise((res) => {
    const remove = consumer.on(consumer.events.GROUP_JOIN, () => {
      remove();
      res();
    });
  });
}

async function getTopicInd(admin: Admin): Promise<number> {
  const topics = await admin.listTopics();
  const testTopics = topics.filter((it) => it.startsWith('test_'));
  console.log('Test topics count: ', testTopics.length);
  return testTopics.length;
}

async function createTopic(admin: Admin, topic: string): Promise<void> {
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: 3,
      },
    ],
  });
}

const metaStore = new ConsumerMetaStorage();

async function createConsumer(id: string): Promise<Consumer> {
  const consumer = kafka.consumer({
    groupId: GROUP_ID,
  });
  metaStore.setupStore(id, consumer);
  const groupJoin = waitForGroupJoin(consumer);
  await consumer.connect();
  await consumer.subscribe({
    topic: /test_.*/,
  });
  await consumer.run({
    eachMessage: async () => {},
  });
  await groupJoin;
  return consumer;
}

async function main(): Promise<void> {
  const admin = kafka.admin();
  const startInd = await getTopicInd(admin);
  console.log('New topic will start with ind: ', startInd);
  await createTopic(admin, `test_${startInd}`);
  const consumer1 = await createConsumer('one');
  console.log('consumer 1 joined the group');
  const newTopic = `test_${startInd + 1}`;
  await createTopic(admin, newTopic);
  const consumer2 = await createConsumer('two');
  console.log('consumer 2 joined the group');
  await new Promise((res) => setTimeout(res, 1000));
  // do not handle other updates we shutdown the group
  metaStore.stop();
  await Promise.all([consumer1.disconnect(), consumer2.disconnect()]);
  await admin.disconnect();
  const oneAssignment = metaStore.store.one.memberAssignment;
  const twoAssignment = metaStore.store.two.memberAssignment;
  console.log(oneAssignment, twoAssignment);
  console.log('One has new topic: ', Object.keys(oneAssignment).includes(newTopic))
  console.log('Two has new topic: ', Object.keys(oneAssignment).includes(newTopic))
}

main().catch(console.error.bind(console));
