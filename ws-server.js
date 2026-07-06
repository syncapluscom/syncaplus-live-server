// SYNCA+ Stage 6 - WebSocket Live Server
// Kurulum:
// npm install
// npm start
//
// Varsayılan port: 3000
// Render/Railway gibi servislerde PORT env otomatik kullanılır.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const API_SECRET = process.env.SYNCA_WS_SECRET || "change-this-secret";

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      ok: true,
      service: "SYNCA+ WebSocket Live Server",
      clients: clients.size,
      events: eventClients.size
    }));
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify({ ok: false, message: "Not found" }));
});

const wss = new WebSocketServer({ server });

const clients = new Map(); // ws -> meta
const eventClients = new Map(); // eventCode -> Set(ws)

function safeSend(ws, payload) {
  if (ws.readyState !== ws.OPEN) return false;
  ws.send(JSON.stringify(payload));
  return true;
}

function addToEvent(eventCode, ws) {
  if (!eventClients.has(eventCode)) eventClients.set(eventCode, new Set());
  eventClients.get(eventCode).add(ws);
}

function removeClient(ws) {
  const meta = clients.get(ws);
  if (meta?.eventCode && eventClients.has(meta.eventCode)) {
    eventClients.get(meta.eventCode).delete(ws);
    if (eventClients.get(meta.eventCode).size === 0) {
      eventClients.delete(meta.eventCode);
    }
  }
  clients.delete(ws);
}

function broadcastToEvent(eventCode, payload) {
  const set = eventClients.get(eventCode);
  if (!set) return 0;

  let count = 0;
  for (const ws of set) {
    if (safeSend(ws, payload)) count++;
  }

  return count;
}

function createPatternCommand(pattern, calibration = {}) {
  const startAt = Date.now() + 1200; // tüm cihazlar için ortak başlangıç
  return {
    type: "pattern",
    id: "cmd_" + Date.now() + "_" + Math.random().toString(16).slice(2),
    startAt,
    calibration: {
      ios: Number(calibration.ios || 0),
      android: Number(calibration.android || 0),
      default: Number(calibration.default || 0)
    },
    pattern
  };
}

wss.on("connection", (ws) => {
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      safeSend(ws, { type: "error", message: "Invalid JSON" });
      return;
    }

    if (msg.type === "join") {
      const eventCode = String(msg.eventCode || "").trim();

      if (!eventCode) {
        safeSend(ws, { type: "error", message: "eventCode missing" });
        return;
      }

      removeClient(ws);

      const meta = {
        role: "phone",
        eventCode,
        deviceToken: String(msg.deviceToken || ""),
        ua: String(msg.ua || "")
      };

      clients.set(ws, meta);
      addToEvent(eventCode, ws);

      safeSend(ws, {
        type: "joined",
        eventCode,
        serverTime: Date.now(),
        online: eventClients.get(eventCode)?.size || 0
      });

      broadcastToEvent(eventCode, {
        type: "online",
        online: eventClients.get(eventCode)?.size || 0
      });

      return;
    }

    if (msg.type === "control") {
      if (String(msg.secret || "") !== API_SECRET) {
        safeSend(ws, { type: "error", message: "unauthorized" });
        return;
      }

      const eventCode = String(msg.eventCode || "").trim();
      if (!eventCode) {
        safeSend(ws, { type: "error", message: "eventCode missing" });
        return;
      }

      let command;

      if (msg.command === "audio_pattern") {
        command = {
          type: "pattern",
          id: "cmd_" + Date.now() + "_" + Math.random().toString(16).slice(2),
          startAt: Date.now() + Number(msg.leadMs || 180),
          calibration: {
            ios: Number(msg.calibration?.ios || 0),
            android: Number(msg.calibration?.android || 0),
            default: Number(msg.calibration?.default || 0)
          },
          pattern: msg.pattern || {
            name: "Audio Pulse",
            steps: [
              { state: "on", duration: 90 },
              { state: "off", duration: 70 }
            ]
          }
        };
      } else if (msg.command === "flash_test") {
        command = {
          type: "flash_test",
          id: "cmd_" + Date.now() + "_" + Math.random().toString(16).slice(2),
          startAt: Date.now() + 900,
          duration: Number(msg.duration || 350),
          calibration: {
            ios: Number(msg.calibration?.ios || 0),
            android: Number(msg.calibration?.android || 0),
            default: Number(msg.calibration?.default || 0)
          }
        };
      } else {
        command = createPatternCommand(msg.pattern || {
          name: "Default",
          steps: [
            { state: "on", duration: 180 },
            { state: "off", duration: 120 },
            { state: "on", duration: 180 }
          ]
        }, msg.calibration || {});
      }

      const sent = broadcastToEvent(eventCode, command);

      safeSend(ws, {
        type: "sent",
        eventCode,
        sent,
        command
      });

      return;
    }

    if (msg.type === "ping") {
      safeSend(ws, { type: "pong", serverTime: Date.now() });
      return;
    }
  });

  ws.on("close", () => {
    const meta = clients.get(ws);
    const eventCode = meta?.eventCode;
    removeClient(ws);

    if (eventCode) {
      broadcastToEvent(eventCode, {
        type: "online",
        online: eventClients.get(eventCode)?.size || 0
      });
    }
  });

  ws.on("error", () => {
    removeClient(ws);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      removeClient(ws);
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`SYNCA+ WebSocket Live Server running on :${PORT}`);
});
