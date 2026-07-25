const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const SECRET = process.env.SYNCA_WS_SECRET || "change-this-secret";

const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();
const roomStats = new Map();
const seenDevices = new Map();

function getRoomStats(code) {
  const eventCode = normalizeCode(code);
  if (!roomStats.has(eventCode)) {
    roomStats.set(eventCode, {
      joined: 0, reconnects: 0, disconnects: 0,
      commands: 0, ackTotal: 0,
      lastCommandId: null, lastSent: 0, lastAck: 0,
      lastBroadcastDurationMs: 0, lastBroadcastAt: null
    });
  }
  return roomStats.get(eventCode);
}

const serverStats = {
  startedAt: Date.now(),
  totalConnections: 0,
  totalMessages: 0,
  totalBroadcasts: 0,
  lastBroadcastAt: null,
  lastBroadcastSent: 0
};

function normalizeCode(code) {
  return String(code || "").trim().toUpperCase();
}

function getRoom(code) {
  const eventCode = normalizeCode(code);
  if (!rooms.has(eventCode)) rooms.set(eventCode, new Set());
  return rooms.get(eventCode);
}

function getRoomPhoneOnline(code) {
  const room = rooms.get(normalizeCode(code));
  if (!room) return 0;
  let count = 0;
  for (const client of room) {
    if (client.role === "phone" && client.readyState === WebSocket.OPEN) count++;
  }
  return count;
}

function getRoomOnline(code) {
  const room = rooms.get(normalizeCode(code));
  if (!room) return 0;

  let count = 0;
  for (const client of room) {
    if (client.readyState === WebSocket.OPEN) count++;
  }
  return count;
}

function roomBroadcast(code, payload, except = null) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return 0;

  const data = JSON.stringify(payload);
  let sent = 0;

  for (const client of room) {
    if (client !== except && client.role === "phone" && client.readyState === WebSocket.OPEN) {
      client.send(data);
      sent++;
    }
  }

  return sent;
}

function sendAudioStatusToPanels(code, extra = {}) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;

  let source = null;
  for (const client of room) {
    if (client.role === "audio_source" && client.readyState === WebSocket.OPEN) {
      source = client;
      break;
    }
  }

  const payload = JSON.stringify({
    type: "audio_source_status",
    eventCode,
    online: !!source,
    sourceLabel: source?.sourceLabel || extra.sourceLabel || "",
    modeLabel: source?.modeLabel || extra.modeLabel || "",
    serverTime: Date.now()
  });

  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function sendOnlineToPanels(code) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;

  const online = getRoomOnline(eventCode);
  const payload = JSON.stringify({
    type: "online",
    eventCode,
    online,
    serverTime: Date.now()
  });

  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

function sendPerformanceToPanels(code) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;
  const st = getRoomStats(eventCode);
  const payload = JSON.stringify({
    type: "performance",
    eventCode,
    phoneOnline: getRoomPhoneOnline(eventCode),
    joined: st.joined,
    reconnects: st.reconnects,
    disconnects: st.disconnects,
    commands: st.commands,
    lastCommandId: st.lastCommandId,
    lastSent: st.lastSent,
    lastAck: st.lastAck,
    pending: Math.max(0, st.lastSent - st.lastAck),
    deliveryRate: st.lastSent ? Number(((st.lastAck / st.lastSent) * 100).toFixed(2)) : 100,
    broadcastDurationMs: st.lastBroadcastDurationMs,
    lastBroadcastAt: st.lastBroadcastAt,
    serverTime: Date.now()
  });
  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function sendHealth(ws, code) {
  const eventCode = normalizeCode(code || ws.eventCode);
  if (!eventCode || ws.readyState !== WebSocket.OPEN) return;

  ws.send(JSON.stringify({
    type: "health",
    eventCode,
    online: getRoomOnline(eventCode),
    phoneOnline: getRoomPhoneOnline(eventCode),
    uptimeSec: Math.round((Date.now() - serverStats.startedAt) / 1000),
    totalConnections: serverStats.totalConnections,
    totalMessages: serverStats.totalMessages,
    totalBroadcasts: serverStats.totalBroadcasts,
    lastBroadcastAt: serverStats.lastBroadcastAt,
    lastBroadcastSent: serverStats.lastBroadcastSent,
    serverTime: Date.now()
  }));
}

function attachClient(ws, code, role) {
  const eventCode = normalizeCode(code);
  if (!eventCode) return false;

  if (ws.eventCode && rooms.has(ws.eventCode)) {
    rooms.get(ws.eventCode).delete(ws);
  }

  ws.eventCode = eventCode;
  ws.role = role || "phone";
  getRoom(eventCode).add(ws);
  sendOnlineToPanels(eventCode);
  return true;
}

function createPatternCommand(msg) {
  const now = Date.now();
  const startAt = Number(msg.startAt || 0) || (now + Number(msg.leadMs || 180));

  return {
    type: "pattern",
    id: "cmd_" + now + "_" + Math.random().toString(16).slice(2),
    startAt,
    pattern: msg.pattern || {
      name: "Flash",
      steps: [{ state: "on", duration: 250 }, { state: "off", duration: 120 }]
    },
    calibration: {
      ios: Number(msg.calibration?.ios || 0),
      android: Number(msg.calibration?.android || 0),
      default: Number(msg.calibration?.default || 0)
    }
  };
}

function createStopCommand(msg) {
  const now = Date.now();
  return {
    type: "stop",
    id: "cmd_" + now + "_" + Math.random().toString(16).slice(2),
    startAt: now + 60,
    calibration: {
      ios: Number(msg.calibration?.ios || 0),
      android: Number(msg.calibration?.android || 0),
      default: Number(msg.calibration?.default || 0)
    }
  };
}

function createFlashTestCommand(msg) {
  const now = Date.now();
  return {
    type: "flash_test",
    id: "cmd_" + now + "_" + Math.random().toString(16).slice(2),
    startAt: now + Number(msg.leadMs || 180),
    duration: Number(msg.duration || 350),
    calibration: {
      ios: Number(msg.calibration?.ios || 0),
      android: Number(msg.calibration?.android || 0),
      default: Number(msg.calibration?.default || 0)
    }
  };
}

wss.on("connection", (ws) => {
  serverStats.totalConnections++;

  ws.isAlive = true;
  ws.role = "unknown";
  ws.eventCode = "";

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    serverStats.totalMessages++;

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      ws.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
      return;
    }

    const type = msg.type;

    if (type === "ping") {
      ws.send(JSON.stringify({
        type: "pong",
        clientTime: msg.clientTime || null,
        serverTime: Date.now(),
        eventCode: ws.eventCode || normalizeCode(msg.eventCode)
      }));
      return;
    }

    if (type === "health_request") {
      sendHealth(ws, msg.eventCode || ws.eventCode);
      return;
    }

    if (type === "panel_join") {
      const eventCode = normalizeCode(msg.eventCode);
      if (!eventCode) {
        ws.send(JSON.stringify({ type: "error", message: "Event code missing" }));
        return;
      }

      attachClient(ws, eventCode, "panel");
      ws.send(JSON.stringify({
        type: "panel_joined",
        eventCode,
        online: getRoomOnline(eventCode),
        phoneOnline: getRoomPhoneOnline(eventCode),
        serverTime: Date.now()
      }));
      sendHealth(ws, eventCode);
      sendAudioStatusToPanels(eventCode);
      return;
    }

    if (type === "audio_source_join") {
      const eventCode = normalizeCode(msg.eventCode);
      if (!eventCode || msg.secret !== SECRET) {
        ws.send(JSON.stringify({ type: "error", message: "Unauthorized audio source" }));
        return;
      }
      attachClient(ws, eventCode, "audio_source");
      ws.sourceLabel = String(msg.sourceLabel || "Ses bilgisayarı");
      ws.modeLabel = String(msg.modeLabel || "Hazır");
      ws.send(JSON.stringify({ type: "audio_source_joined", eventCode, serverTime: Date.now() }));
      sendAudioStatusToPanels(eventCode);
      return;
    }

    if (type === "audio_source_heartbeat") {
      if (ws.role !== "audio_source") return;
      ws.sourceLabel = String(msg.sourceLabel || ws.sourceLabel || "Ses bilgisayarı");
      ws.modeLabel = String(msg.modeLabel || ws.modeLabel || "Aktif");
      sendAudioStatusToPanels(ws.eventCode);
      return;
    }

    if (type === "join") {
      const eventCode = normalizeCode(msg.eventCode);
      if (!eventCode) {
        ws.send(JSON.stringify({ type: "error", message: "Event code missing" }));
        return;
      }

      attachClient(ws, eventCode, "phone");
      ws.deviceToken = msg.deviceToken || ("anon_" + Math.random().toString(16).slice(2));
      const st = getRoomStats(eventCode);
      if (!seenDevices.has(eventCode)) seenDevices.set(eventCode, new Set());
      const devices = seenDevices.get(eventCode);
      if (devices.has(ws.deviceToken)) st.reconnects++; else { devices.add(ws.deviceToken); st.joined++; }

      ws.ua = msg.ua || "";

      ws.send(JSON.stringify({
        type: "joined",
        eventCode,
        serverTime: Date.now(),
        online: getRoomOnline(eventCode)
      }));

      sendPerformanceToPanels(eventCode);
      return;
    }

    if (type === "ack") {
      const eventCode = normalizeCode(msg.eventCode || ws.eventCode);
      if (!eventCode || ws.role !== "phone") return;
      const st = getRoomStats(eventCode);
      if (msg.commandId && msg.commandId === st.lastCommandId && ws.lastAckCommandId !== msg.commandId) {
        ws.lastAckCommandId = msg.commandId;
        st.lastAck++;
        st.ackTotal++;
        if (st.lastAck === st.lastSent || st.lastAck % 25 === 0) sendPerformanceToPanels(eventCode);
      }
      return;
    }

    if (type === "control") {
      if (msg.secret !== SECRET) {
        ws.send(JSON.stringify({ type: "error", message: "Unauthorized" }));
        return;
      }

      const eventCode = normalizeCode(msg.eventCode || ws.eventCode);
      if (!eventCode) {
        ws.send(JSON.stringify({ type: "error", message: "Event code missing" }));
        return;
      }

      let command;
      if (msg.command === "stop_loop") {
        command = createStopCommand(msg);
      } else if (msg.command === "flash_test") {
        command = createFlashTestCommand(msg);
      } else {
        command = createPatternCommand(msg);
      }

      const broadcastStarted = process.hrtime.bigint();
      const sent = roomBroadcast(eventCode, command, null);
      const broadcastDurationMs = Number(process.hrtime.bigint() - broadcastStarted) / 1e6;
      const st = getRoomStats(eventCode);
      st.commands++;
      st.lastCommandId = command.id;
      st.lastSent = sent;
      st.lastAck = 0;
      st.lastBroadcastDurationMs = Number(broadcastDurationMs.toFixed(3));
      st.lastBroadcastAt = Date.now();

      serverStats.totalBroadcasts++;
      serverStats.lastBroadcastAt = Date.now();
      serverStats.lastBroadcastSent = sent;

      ws.send(JSON.stringify({
        type: "sent",
        sent,
        eventCode,
        commandType: command.type,
        serverTime: Date.now(),
        commandId: command.id,
        broadcastDurationMs: st.lastBroadcastDurationMs
      }));

      sendOnlineToPanels(eventCode);
      sendPerformanceToPanels(eventCode);
      return;
    }

    ws.send(JSON.stringify({ type: "error", message: "Unknown message type: " + type }));
  });

  ws.on("close", () => {
    if (ws.eventCode && rooms.has(ws.eventCode)) {
      const eventCode = ws.eventCode;
      rooms.get(eventCode).delete(ws);
      if (ws.role === "phone") getRoomStats(eventCode).disconnects++;
      sendOnlineToPanels(eventCode);
      sendPerformanceToPanels(eventCode);
      sendAudioStatusToPanels(eventCode);
    }
  });

  ws.on("error", () => {});
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (e) {}
      continue;
    }

    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

setInterval(() => {
  for (const [eventCode, room] of rooms.entries()) {
    for (const client of [...room]) {
      if (client.readyState !== WebSocket.OPEN) room.delete(client);
    }
    sendOnlineToPanels(eventCode);
    sendPerformanceToPanels(eventCode);
  }
}, 5000);

console.log("SYNCA+ WebSocket Live Server running on :" + PORT);
