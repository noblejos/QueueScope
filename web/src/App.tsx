import { useEffect, useState } from "react";
import {
  Activity,
  Database,
  Lock,
  LogOut,
  PlugZap,
  RefreshCw,
  ShieldCheck,
  Trash2
} from "lucide-react";
import {
  AuditLogEntry,
  ConnectionHealth,
  ConnectionMode,
  createConnection,
  deleteConnection,
  getAuditLog,
  getConnections,
  getMe,
  getMessages,
  getProviders,
  getQueues,
  login,
  logout,
  MessageStatus,
  ProviderInfo,
  QueueConnection,
  QueueInfo,
  QueueMessage,
  removeMessage,
  retryMessage,
  testConnection,
  User
} from "./lib/api";

const defaultEmail = "admin@queuescope.local";
const providerConfigTemplates: Record<ProviderInfo["id"], string> = {
  bullmq: JSON.stringify({ redisUrl: "redis://localhost:6379", prefix: "bull" }, null, 2),
  sqs: JSON.stringify(
    {
      region: "us-east-1",
      queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
      profile: "default"
    },
    null,
    2
  ),
  rabbitmq: JSON.stringify(
    {
      amqpUrl: "amqp://queuescope:queuescope@localhost:5672",
      vhost: "/"
    },
    null,
    2
  )
};

const providerConfigHints: Record<ProviderInfo["id"], string> = {
  bullmq: "Requires redisUrl. Optional prefix defaults to bull.",
  sqs: "Requires region and queueUrl. Optional profile or endpointUrl can be included.",
  rabbitmq: "Requires amqpUrl. Optional vhost defaults to /."
};

export function App() {
  const [user, setUser] = useState<User | null>(null);
  const [providers, setProviders] = useState<ProviderInfo[]>([]);
  const [connections, setConnections] = useState<QueueConnection[]>([]);
  const [connectionName, setConnectionName] = useState("Local BullMQ");
  const [connectionProvider, setConnectionProvider] = useState<ProviderInfo["id"]>("bullmq");
  const [connectionMode, setConnectionMode] = useState<ConnectionMode>("read_only");
  const [connectionConfig, setConnectionConfig] = useState(providerConfigTemplates.bullmq);
  const [health, setHealth] = useState<Record<string, ConnectionHealth>>({});
  const [selectedConnectionId, setSelectedConnectionId] = useState("");
  const [queues, setQueues] = useState<QueueInfo[]>([]);
  const [selectedQueue, setSelectedQueue] = useState("");
  const [messageStatus, setMessageStatus] = useState<MessageStatus>("failed");
  const [messages, setMessages] = useState<QueueMessage[]>([]);
  const [queueLoading, setQueueLoading] = useState(false);
  const [messageLoading, setMessageLoading] = useState(false);
  const [auditEntries, setAuditEntries] = useState<AuditLogEntry[]>([]);
  const [email, setEmail] = useState(defaultEmail);
  const [password, setPassword] = useState("queuescope");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getMe()
      .then((result) => setUser(result.user))
      .catch(() => setUser(null))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!user) {
      setProviders([]);
      return;
    }

    getProviders()
      .then((result) => setProviders(result.providers))
      .catch((err) => setError(err.message));
    getConnections()
      .then((result) => setConnections(result.connections))
      .catch((err) => setError(err.message));
    getAuditLog()
      .then((result) => setAuditEntries(result.entries))
      .catch((err) => setError(err.message));
  }, [user]);

  async function handleLogin(event: React.FormEvent) {
    event.preventDefault();
    setError("");

    try {
      const result = await login(email, password);
      setUser(result.user);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    }
  }

  async function handleLogout() {
    await logout();
    setUser(null);
  }

  async function handleCreateConnection(event: React.FormEvent) {
    event.preventDefault();
    setError("");

    let config: Record<string, unknown>;
    try {
      config = JSON.parse(connectionConfig) as Record<string, unknown>;
    } catch {
      setError("Connection config must be valid JSON");
      return;
    }

    try {
      const result = await createConnection({
        name: connectionName,
        provider: connectionProvider,
        mode: connectionMode,
        config
      });
      setConnections((current) => [...current, result.connection]);
      setSelectedConnectionId(result.connection.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not create connection");
    }
  }

  async function handleLoadQueues(connectionId = selectedConnectionId) {
    if (!connectionId) {
      setError("Choose a connection first");
      return;
    }

    setError("");
    setQueueLoading(true);
    try {
      const result = await getQueues(connectionId);
      setQueues(result.queues);
      setSelectedConnectionId(connectionId);
      const firstQueue = result.queues[0]?.name ?? "";
      setSelectedQueue(firstQueue);
      setMessages([]);
      if (firstQueue) {
        await loadMessages(connectionId, firstQueue, messageStatus);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not load queues");
    } finally {
      setQueueLoading(false);
    }
  }

  async function handleLoadMessages(queueName = selectedQueue) {
    if (!selectedConnectionId || !queueName) {
      setError("Choose a connection and queue first");
      return;
    }

    setError("");
    await loadMessages(selectedConnectionId, queueName, messageStatus);
  }

  async function loadMessages(connectionId: string, queueName: string, status: MessageStatus) {
    setMessageLoading(true);
    try {
      const result = await getMessages(connectionId, queueName, status);
      setMessages(result.messages);
      setSelectedQueue(queueName);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not load messages");
    } finally {
      setMessageLoading(false);
    }
  }

  async function refreshAuditLog() {
    const result = await getAuditLog();
    setAuditEntries(result.entries);
  }

  async function handleRetryMessage(message: QueueMessage) {
    if (!selectedConnectionId) {
      return;
    }
    if (!confirm(`Retry job ${message.id}?`)) {
      return;
    }

    setError("");
    try {
      await retryMessage(selectedConnectionId, message.queueName, message.id);
      await handleLoadMessages(message.queueName);
      await refreshAuditLog();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not retry message");
      await refreshAuditLog();
    }
  }

  async function handleRemoveMessage(message: QueueMessage) {
    if (!selectedConnectionId) {
      return;
    }
    if (!confirm(`Delete job ${message.id}?`)) {
      return;
    }

    setError("");
    try {
      await removeMessage(selectedConnectionId, message.queueName, message.id);
      setMessages((current) => current.filter((candidate) => candidate.id !== message.id));
      await refreshAuditLog();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not delete message");
      await refreshAuditLog();
    }
  }

  async function handleTestConnection(connectionId: string) {
    setError("");
    try {
      const result = await testConnection(connectionId);
      setHealth((current) => ({ ...current, [connectionId]: result }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Connection test failed");
    }
  }

  async function handleDeleteConnection(connectionId: string) {
    setError("");
    try {
      await deleteConnection(connectionId);
      setConnections((current) => current.filter((connection) => connection.id !== connectionId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not delete connection");
    }
  }

  if (loading) {
    return (
      <main className="loading">
        <RefreshCw className="spin" size={22} />
      </main>
    );
  }

  if (!user) {
    return (
      <main className="auth-page">
        <section className="auth-panel">
          <div className="brand-mark">
            <Activity size={28} />
          </div>
          <h1>QueueScope</h1>
          <p>Sign in to inspect queues, messages, and provider capabilities.</p>

          <form onSubmit={handleLogin}>
            <label>
              Email
              <input value={email} onChange={(event) => setEmail(event.target.value)} />
            </label>
            <label>
              Password
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
            </label>
            {error && <div className="error">{error}</div>}
            <button type="submit">
              <Lock size={16} />
              Sign in
            </button>
          </form>
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <Activity size={24} />
          <span>QueueScope</span>
        </div>
        <nav>
          <a className="active">Queues</a>
          <a>Connections</a>
          <a>Audit Log</a>
        </nav>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <h1>Queue Operations</h1>
            <p>Create provider connections, validate them, and build toward live queue inspection.</p>
          </div>
          <div className="user-pill">
            <ShieldCheck size={16} />
            <span>{user.email}</span>
            <button aria-label="Sign out" onClick={handleLogout}>
              <LogOut size={16} />
            </button>
          </div>
        </header>

        {error && <div className="error workspace-error">{error}</div>}

        <section className="split-layout">
          <form className="connection-form" onSubmit={handleCreateConnection}>
            <div className="section-heading">
              <h2>Add Connection</h2>
              <p>Saved in Postgres and used by provider adapters.</p>
            </div>

            <label>
              Name
              <input
                value={connectionName}
                onChange={(event) => setConnectionName(event.target.value)}
              />
            </label>

            <label>
              Provider
              <select
                value={connectionProvider}
                onChange={(event) => {
                  const provider = event.target.value as ProviderInfo["id"];
                  setConnectionProvider(provider);
                  setConnectionConfig(providerConfigTemplates[provider]);
                  setConnectionName(defaultConnectionName(provider));
                }}
              >
                {providers.map((provider) => (
                  <option key={provider.id} value={provider.id}>
                    {provider.name}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Mode
              <select
                value={connectionMode}
                onChange={(event) => setConnectionMode(event.target.value as ConnectionMode)}
              >
                <option value="read_only">Read-only</option>
                <option value="operator">Operator</option>
              </select>
            </label>

            <label>
              Config JSON
              <textarea
                rows={7}
                value={connectionConfig}
                onChange={(event) => setConnectionConfig(event.target.value)}
              />
              <span className="field-hint">{providerConfigHints[connectionProvider]}</span>
            </label>

            <button type="submit">
              <PlugZap size={16} />
              Add connection
            </button>
          </form>

          <section className="connections-panel">
            <div className="section-heading">
              <h2>Connections</h2>
              <p>{connections.length} configured</p>
            </div>

            <div className="connection-list">
              {connections.length === 0 && <div className="empty-state">No connections yet.</div>}
              {connections.map((connection) => (
                <article className="connection-row" key={connection.id}>
                  <div>
                    <h3>{connection.name}</h3>
                    <p>
                      {connection.provider} / {connection.mode.replace("_", "-")}
                    </p>
                    {health[connection.id] && (
                      <span className={`health ${health[connection.id].status}`}>
                        {health[connection.id].status}
                      </span>
                    )}
                  </div>
                  <div className="row-actions">
                    <button type="button" onClick={() => handleTestConnection(connection.id)}>
                      <RefreshCw size={15} />
                      Test
                    </button>
                    <button type="button" onClick={() => handleLoadQueues(connection.id)}>
                      <Database size={15} />
                      Queues
                    </button>
                    <button
                      type="button"
                      className="icon-danger"
                      aria-label={`Delete ${connection.name}`}
                      onClick={() => handleDeleteConnection(connection.id)}
                    >
                      <Trash2 size={15} />
                    </button>
                  </div>
                </article>
              ))}
            </div>
          </section>
        </section>

        <section className="queue-browser">
          <div className="section-heading">
            <h2>Queue Browser</h2>
            <p>Load queues from a saved connection and inspect BullMQ jobs.</p>
          </div>

          <div className="browser-controls">
            <label>
              Connection
              <select
                value={selectedConnectionId}
                onChange={(event) => setSelectedConnectionId(event.target.value)}
              >
                <option value="">Choose connection</option>
                {connections.map((connection) => (
                  <option key={connection.id} value={connection.id}>
                    {connection.name}
                  </option>
                ))}
              </select>
            </label>
            <button type="button" onClick={() => handleLoadQueues()}>
              <RefreshCw size={15} />
              {queueLoading ? "Loading..." : "Load queues"}
            </button>
          </div>

          <div className="queue-grid">
            <div className="queue-list">
              {queues.length === 0 && (
                <div className="empty-state">
                  No queues found. Seed BullMQ demo data, then load queues again.
                </div>
              )}
              {queues.map((queue) => (
                <button
                  type="button"
                  className={queue.name === selectedQueue ? "queue-item active" : "queue-item"}
                  key={queue.id}
                  onClick={() => {
                    setSelectedQueue(queue.name);
                    void loadMessages(selectedConnectionId, queue.name, messageStatus);
                  }}
                >
                  <strong>{queue.name}</strong>
                  <span>
                    waiting {queue.stats.waiting ?? 0} / failed {queue.stats.failed ?? 0}
                  </span>
                </button>
              ))}
            </div>

            <div className="message-panel">
              <div className="message-toolbar">
                <select
                  value={messageStatus}
                  onChange={(event) => setMessageStatus(event.target.value as MessageStatus)}
                >
                  <option value="failed">Failed</option>
                  <option value="waiting">Waiting</option>
                  <option value="active">Active</option>
                  <option value="delayed">Delayed</option>
                  <option value="completed">Completed</option>
                </select>
                <button type="button" onClick={() => handleLoadMessages()}>
                  <RefreshCw size={15} />
                  {messageLoading ? "Loading..." : "Load messages"}
                </button>
              </div>

              <div className="message-list">
                {messages.length === 0 && (
                  <div className="empty-state">
                    No {messageStatus} messages found for the selected queue.
                  </div>
                )}
                {messages.map((message) => (
                  <article className="message-row" key={`${message.queueName}-${message.id}`}>
                    <header>
                      <div>
                        <h3>{message.id}</h3>
                        <p>
                          {message.status} / attempts {message.attempts ?? 0}
                        </p>
                      </div>
                      {message.error && <span className="message-error">{message.error}</span>}
                    </header>
                    <div className="message-actions">
                      <button
                        type="button"
                        disabled={message.status !== "failed"}
                        onClick={() => handleRetryMessage(message)}
                      >
                        <RefreshCw size={15} />
                        Retry
                      </button>
                      <button
                        type="button"
                        className="danger-action"
                        onClick={() => handleRemoveMessage(message)}
                      >
                        <Trash2 size={15} />
                        Delete
                      </button>
                    </div>
                    <pre>{JSON.stringify(message.payload, null, 2)}</pre>
                  </article>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section className="provider-grid">
          {providers.map((provider) => (
            <article className="provider-card" key={provider.id}>
              <div className="provider-icon">
                <Database size={20} />
              </div>
              <div>
                <h2>{provider.name}</h2>
                <p>{provider.capabilities.length} capabilities</p>
              </div>
              <ul>
                {provider.capabilities.slice(0, 6).map((capability) => (
                  <li key={capability}>{capability}</li>
                ))}
              </ul>
            </article>
          ))}
        </section>

        <section className="audit-panel">
          <div className="section-heading">
            <h2>Audit Log</h2>
            <p>{auditEntries.length} recent operator actions</p>
          </div>

          <div className="audit-list">
            {auditEntries.length === 0 && <div className="empty-state">No audit entries yet.</div>}
            {auditEntries.map((entry) => (
              <article className="audit-row" key={entry.id}>
                <div>
                  <h3>
                    {entry.action.replace("_", " ")} / {entry.result}
                  </h3>
                  <p>
                    {entry.queueName} / {entry.messageId} / {entry.actorEmail}
                  </p>
                </div>
                {entry.error && <span className="message-error">{entry.error}</span>}
              </article>
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}

function defaultConnectionName(provider: ProviderInfo["id"]) {
  switch (provider) {
    case "bullmq":
      return "Local BullMQ";
    case "sqs":
      return "Production SQS";
    case "rabbitmq":
      return "Local RabbitMQ";
  }
}
