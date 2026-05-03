import { Queue } from "bullmq";
import { queueName, redisConnection } from "./config.js";

const queue = new Queue(queueName, {
  connection: redisConnection,
  prefix: "bull"
});

await queue.obliterate({ force: true });
console.log(`obliterated BullMQ queue ${queueName}`);

await queue.close();

