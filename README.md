# QueueScope

QueueScope is a developer dashboard for inspecting, debugging, and safely operating jobs/messages across queue systems.

The project is starting as a multi-provider queue observability and control plane. The MVP targets BullMQ/Redis, Amazon SQS, and RabbitMQ.

## Stack

- Backend: Go + Gin
- Frontend: React + TypeScript + Vite
- Database: Postgres
- Auth: local session auth from the start
- Queue integrations: provider adapters

## Development

Start local services:

```sh
docker compose up -d postgres redis rabbitmq
docker compose ps
```

If Postgres is still starting, the backend will wait and retry briefly before failing.

Run the backend:

```sh
go run ./cmd/queuescope
```

Run the frontend:

```sh
cd web
npm install
npm run dev
```

Seed local BullMQ demo data:

```sh
cd demo/bullmq
npm install
npm run seed
npm run worker
```

The worker intentionally fails jobs where `shouldFail` is `true`. Stop it with `Ctrl+C` after it processes a few jobs.

Use this QueueScope connection config for the demo:

```json
{
  "redisUrl": "redis://localhost:6379",
  "prefix": "bull"
}
```

## Connection Configs

BullMQ / Redis:

```json
{
  "redisUrl": "redis://localhost:6379",
  "prefix": "bull"
}
```

Amazon SQS:

```json
{
  "region": "us-east-1",
  "queueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
  "profile": "default"
}
```

RabbitMQ:

```json
{
  "amqpUrl": "amqp://queuescope:queuescope@localhost:5672",
  "vhost": "/"
}
```

Clear the demo queue:

```sh
cd demo/bullmq
npm run drain
```

Default local login:

```txt
Email: admin@queuescope.local
Password: queuescope
```

Override these values with environment variables:

```sh
QUEUESCOPE_ADMIN_EMAIL=you@example.com
QUEUESCOPE_ADMIN_PASSWORD='change-me'
QUEUESCOPE_SESSION_SECRET='long-random-secret'
go run ./cmd/queuescope
```

## Product Spec

See [SPEC.md](./SPEC.md).

## Current API Surface

```txt
GET    /api/health
POST   /api/auth/login
POST   /api/auth/logout
GET    /api/auth/me
GET    /api/providers
GET    /api/connections
POST   /api/connections
GET    /api/connections/:connectionId/health
DELETE /api/connections/:connectionId
GET    /api/connections/:connectionId/queues
GET    /api/connections/:connectionId/queues/:queueName/messages
GET    /api/connections/:connectionId/queues/:queueName/messages/:messageId
POST   /api/connections/:connectionId/queues/:queueName/messages/:messageId/retry
DELETE /api/connections/:connectionId/queues/:queueName/messages/:messageId
GET    /api/audit-log
```

Connections are stored in Postgres.
Queue mutations require the connection to be in `operator` mode and are written to the audit log.
