import { Worker } from "bullmq";
import { queueName, redisConnection } from "./config.js";

const worker = new Worker(
  queueName,
  async (job) => {
    console.log(`processing ${job.name} job ${job.id}`);

    if (job.data?.shouldFail) {
      throw new Error(`Demo failure for ${job.name}`);
    }

    await sleep(500);
    return {
      processedAt: new Date().toISOString(),
      jobName: job.name
    };
  },
  {
    connection: redisConnection,
    prefix: "bull",
    concurrency: 2
  }
);

worker.on("completed", (job) => {
  console.log(`completed ${job.name} job ${job.id}`);
});

worker.on("failed", (job, error) => {
  console.log(`failed ${job?.name} job ${job?.id}: ${error.message}`);
});

process.on("SIGINT", async () => {
  await worker.close();
  process.exit(0);
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

