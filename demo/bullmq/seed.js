import { Queue } from "bullmq";
import { queueName, redisConnection } from "./config.js";

const queue = new Queue(queueName, {
  connection: redisConnection,
  prefix: "bull"
});

const now = new Date().toISOString();

const jobs = [
  {
    name: "email.send",
    data: {
      tenantId: "acme",
      recipient: "customer@example.com",
      template: "welcome",
      correlationId: `demo-${Date.now()}-welcome`
    },
    opts: {
      attempts: 3,
      removeOnComplete: false,
      removeOnFail: false
    }
  },
  {
    name: "image.resize",
    data: {
      tenantId: "acme",
      assetId: "asset_42",
      widths: [320, 768, 1440],
      uploadedAt: now
    },
    opts: {
      attempts: 2,
      removeOnComplete: false,
      removeOnFail: false
    }
  },
  {
    name: "report.generate",
    data: {
      tenantId: "globex",
      reportId: "monthly-revenue",
      shouldFail: true,
      requestedAt: now
    },
    opts: {
      attempts: 1,
      removeOnComplete: false,
      removeOnFail: false
    }
  },
  {
    name: "webhook.deliver",
    data: {
      tenantId: "initech",
      endpoint: "https://example.com/hooks/orders",
      eventType: "order.created"
    },
    opts: {
      delay: 1000 * 60 * 5,
      attempts: 5,
      removeOnComplete: false,
      removeOnFail: false
    }
  }
];

for (const job of jobs) {
  const created = await queue.add(job.name, job.data, job.opts);
  console.log(`created ${created.name} job ${created.id}`);
}

await queue.close();

