import http from 'node:http';
import crypto from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import os from 'node:os';

const PORT = Number(process.env.PORT || 8080);
const PAIR_CODE = (process.env.PAIR_CODE || crypto.randomBytes(4).toString('hex'))
  .trim()
  .toUpperCase();

if (!process.env.PAIR_CODE) {
  console.warn('[WARN] PAIR_CODE is not set. A temporary code will be generated for this process.');
  console.warn('[WARN] Set PAIR_CODE in the Render Environment Variables for a stable code.');
}

// This first cloud version keeps the existing one-agent/one-controller pairing model,
// but moves the transport to a public WSS endpoint. Multi-device routing will be added
// in the next server revision once the Android clients carry stable device IDs.
const session = {
  agent: null,
  controller: null,
};

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    });
    res.end(JSON.stringify({
      ok: true,
      service: 'remote-device-cloud-server',
      websocket: true,
      host: os.hostname(),
      time: new Date().toISOString(),
    }));
    return;
  }

  if (req.url === '/') {
    res.writeHead(200, {
      'content-type': 'text/plain; charset=utf-8',
      'cache-control': 'no-store',
    });
    res.end('Remote Device Cloud Server is running.');
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server });

function isOpen(ws) {
  return ws && ws.readyState === WebSocket.OPEN;
}

function sendJson(ws, payload) {
  if (!isOpen(ws)) return false;
  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch (error) {
    console.error('[SEND JSON ERROR]', error);
    return false;
  }
}

function closeExisting(role) {
  const existing = session[role];
  if (!isOpen(existing)) return;

  try {
    existing.close(1000, 'Replaced by a newer connection');
  } catch {}

  session[role] = null;
}

wss.on('connection', (ws, req) => {
  const requestUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const role = requestUrl.searchParams.get('role');
  const code = (requestUrl.searchParams.get('code') || '').trim().toUpperCase();

  if (!['agent', 'controller'].includes(role) || !code || code !== PAIR_CODE) {
    sendJson(ws, { type: 'error', message: 'Unauthorized' });
    ws.close(1008, 'Unauthorized');
    return;
  }

  closeExisting(role);
  session[role] = ws;

  console.log(`[CONNECT] role=${role} remote=${req.socket.remoteAddress}`);
  sendJson(ws, { type: 'hello', role });

  if (isOpen(session.agent) && isOpen(session.controller)) {
    sendJson(session.agent, { type: 'peer', online: true });
    sendJson(session.controller, { type: 'peer', online: true });
  }

  ws.on('message', (raw, isBinary) => {
    const target = role === 'agent' ? session.controller : session.agent;

    if (!isOpen(target)) {
      sendJson(ws, { type: 'peer', online: false });
      return;
    }

    if (isBinary) {
      target.send(raw, { binary: true });
      return;
    }

    // Validate JSON before forwarding so malformed client data does not poison the channel.
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      sendJson(ws, { type: 'error', message: 'Invalid JSON payload' });
      return;
    }

    try {
      target.send(JSON.stringify(message));
    } catch (error) {
      console.error('[FORWARD ERROR]', error);
    }
  });

  ws.on('error', (error) => {
    console.error(`[WS ERROR] role=${role}`, error);
  });

  ws.on('close', (closeCode, reasonBuffer) => {
    if (session[role] === ws) {
      session[role] = null;
    }

    console.log(
      `[CLOSE] role=${role} code=${closeCode} reason=${reasonBuffer?.toString() || ''}`,
    );

    const other = role === 'agent' ? session.controller : session.agent;
    sendJson(other, { type: 'peer', online: false });
  });
});

// Keep connections healthy on hosted infrastructure and allow stale sockets to be removed.
const heartbeatTimer = setInterval(() => {
  for (const role of ['agent', 'controller']) {
    const ws = session[role];
    if (!isOpen(ws)) continue;

    if (ws.isAlive === false) {
      try { ws.terminate(); } catch {}
      continue;
    }

    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      try { ws.terminate(); } catch {}
    }
  }
}, 25000);

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
});

function shutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
  clearInterval(heartbeatTimer);
  try { session.agent?.close(1001, 'Server shutting down'); } catch {}
  try { session.controller?.close(1001, 'Server shutting down'); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Remote Device Cloud Server listening on ${PORT}`);
  console.log(`PAIR_CODE=${PAIR_CODE}`);
});
