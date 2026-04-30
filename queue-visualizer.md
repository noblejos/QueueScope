# Queue Visualizer (QueueScope)

## 🧾 Overview
QueueScope is a developer tool for visualizing, debugging, and managing message queues (SQS, Redis/Bull, Kafka).

It provides real-time visibility into:
- Pending jobs
- Processing jobs
- Failed jobs
- Job payloads and lifecycle

---

## 🎯 Problem
Queues are widely used but:
- No clear visibility into job states
- Debugging failures is difficult
- No easy retry/replay mechanism

---

## 🏗️ Architecture

Producer → Queue → Worker → Result  
                 ↓  
            Event Stream  
                 ↓  
       Visualizer Backend → Frontend UI  

---

## ⚙️ Core Features

### MVP
- Connect to queue (SQS or Redis)
- View jobs by status:
  - pending
  - processing
  - failed
- View job details
- Retry failed jobs

### Advanced
- Replay jobs
- Dead Letter Queue (DLQ) viewer
- Throughput metrics
- Worker health monitoring
- Alerting system

---

## 🧠 Data Model

```ts
type JobStatus = "pending" | "processing" | "completed" | "failed";

interface Job {
  id: string;
  queueName: string;
  status: JobStatus;
  payload: Record<string, any>;
  attempts: number;
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  error?: string;
}
