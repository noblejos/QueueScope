export type User = {
  id: string;
  email: string;
  role: "viewer" | "operator" | "admin";
};

export type ProviderInfo = {
  id: "bullmq" | "sqs" | "rabbitmq";
  name: string;
  capabilities: string[];
};

export type ConnectionMode = "read_only" | "operator";

export type QueueConnection = {
  id: string;
  name: string;
  provider: ProviderInfo["id"];
  mode: ConnectionMode;
  config: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
};

export type ConnectionHealth = {
  status: "healthy" | "unhealthy";
  error?: string;
};

export type QueueStats = {
  waiting?: number;
  active?: number;
  delayed?: number;
  completed?: number;
  failed?: number;
  deadLetter?: number;
  inFlight?: number;
  consumerLag?: number;
};

export type QueueInfo = {
  id: string;
  name: string;
  provider: ProviderInfo["id"];
  connectionId: string;
  stats: QueueStats;
};

export type MessageStatus =
  | "waiting"
  | "active"
  | "delayed"
  | "completed"
  | "failed"
  | "dead_letter"
  | "in_flight"
  | "unknown";

export type QueueMessage = {
  id: string;
  queueName: string;
  provider: ProviderInfo["id"];
  status: MessageStatus;
  payload: unknown;
  metadata: Record<string, unknown>;
  attempts?: number;
  createdAt?: string;
  startedAt?: string;
  completedAt?: string;
  failedAt?: string;
  error?: string;
};

export type AuditLogEntry = {
  id: string;
  actorId: string;
  actorEmail: string;
  action: "retry_message" | "delete_message";
  result: "success" | "failure";
  provider: ProviderInfo["id"];
  connectionId: string;
  queueName: string;
  messageId: string;
  error?: string;
  createdAt: string;
};

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error ?? "Request failed");
  }

  return data as T;
}

export function login(email: string, password: string) {
  return request<{ user: User; expiresAt: string }>("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password })
  });
}

export function logout() {
  return request<{ ok: boolean }>("/api/auth/logout", { method: "POST" });
}

export function getMe() {
  return request<{ user: User }>("/api/auth/me");
}

export function getProviders() {
  return request<{ providers: ProviderInfo[] }>("/api/providers");
}

export function getConnections() {
  return request<{ connections: QueueConnection[] }>("/api/connections");
}

export function createConnection(payload: {
  name: string;
  provider: ProviderInfo["id"];
  mode: ConnectionMode;
  config: Record<string, unknown>;
}) {
  return request<{ connection: QueueConnection }>("/api/connections", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function testConnection(connectionId: string) {
  return request<ConnectionHealth>(`/api/connections/${connectionId}/health`);
}

export function deleteConnection(connectionId: string) {
  return request<{ ok: boolean }>(`/api/connections/${connectionId}`, {
    method: "DELETE"
  });
}

export function getQueues(connectionId: string) {
  return request<{ queues: QueueInfo[] }>(`/api/connections/${connectionId}/queues`);
}

export function getMessages(connectionId: string, queueName: string, status?: MessageStatus) {
  const params = new URLSearchParams();
  if (status) {
    params.set("status", status);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return request<{ messages: QueueMessage[] }>(
    `/api/connections/${connectionId}/queues/${encodeURIComponent(queueName)}/messages${suffix}`
  );
}

export function retryMessage(connectionId: string, queueName: string, messageId: string) {
  return request<{ ok: boolean }>(
    `/api/connections/${connectionId}/queues/${encodeURIComponent(queueName)}/messages/${encodeURIComponent(messageId)}/retry`,
    { method: "POST" }
  );
}

export function removeMessage(connectionId: string, queueName: string, messageId: string) {
  return request<{ ok: boolean }>(
    `/api/connections/${connectionId}/queues/${encodeURIComponent(queueName)}/messages/${encodeURIComponent(messageId)}`,
    { method: "DELETE" }
  );
}

export function getAuditLog() {
  return request<{ entries: AuditLogEntry[] }>("/api/audit-log");
}
