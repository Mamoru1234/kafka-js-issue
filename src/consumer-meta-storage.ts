import { Consumer, RemoveInstrumentationEventListener } from 'kafkajs';

/**
 * just to have more precise picture of consumer metadata
 * Do not affect main logic
 */
export class ConsumerMetaStorage {
  store: {[id: string]: any} = {};
  remove: {[id: string]: RemoveInstrumentationEventListener<any>} = {};

  setupStore(id: string, consumer: Consumer): void {
    this.remove[id] = consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      this.store[id] = payload;
    });
  }

  stop(): void {
    Object.values(this.remove).forEach((it) => it());
  }
}
