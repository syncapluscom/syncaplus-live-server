const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const SECRET = process.env.SYNCA_WS_SECRET || "change-this-secret";

const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();
const roomStats = new Map();
const seenDevices = new Map();
const roomCaps = new Map(); // eventCode -> sayı (Demo: 25) | null (Pro: sınırsız) | tanımsız (henüz panel bağlanmadı)

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
  const targets = [];
  for (const client of room) {
    if (client !== except && client.role === "phone" && client.readyState === WebSocket.OPEN) {
      targets.push(client);
    }
  }

  // Kalabalık (binlerce telefon) varken tek seferde senkron gönderim event loop'u
  // tıkayıp ping/pong işlenmesini geciktiriyordu — sunucu telefonları yanlışlıkla
  // "koptu" sanıp kapatıyordu. Bunun yerine küçük gruplar halinde, aralarında
  // event loop'a nefes payı bırakarak gönderiyoruz.
  const BATCH_SIZE = 250;
  let i = 0;
  function sendBatch() {
    const end = Math.min(i + BATCH_SIZE, targets.length);
    for (; i < end; i++) {
      try { targets[i].send(data); } catch (e) {}
    }
    if (i < targets.length) setImmediate(sendBatch);
  }
  sendBatch();

  return targets.length;
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

// ============ KOLTUK NUMARASI TABANLI IŞIK DALGASI ============
// Her telefona AYNI komut değil, koltuk konumuna göre HESAPLANMIŞ farklı bir
// başlangıç anı (startAt) gönderilir — bu da fiziksel bir ışık dalgası hissi yaratır.
// Koltuk bilgisi hiçbir yerde saklanmaz, sadece bu bağlantı canlıyken bellekte durur.
function broadcastSeatWave(code, msg) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return 0;

  const direction = msg.direction || "left-to-right";
  const color = String(msg.color || "#ffffff");
  const totalDurationMs = Math.max(200, Number(msg.totalDurationMs || 3000));
  const rows = Number(msg.rows || 1);
  const cols = Number(msg.cols || 1);
  const now = Date.now();
  const leadMs = Number(msg.leadMs || 250);

  const targets = [];
  for (const client of room) {
    if (client.role === "phone" && client.readyState === WebSocket.OPEN &&
        Number.isFinite(client.seatRow) && Number.isFinite(client.seatCol)) {
      targets.push(client);
    }
  }
  if (!targets.length) return 0;

  const centerRow = rows / 2;
  const centerCol = cols / 2;
  const maxRadius = Math.sqrt(centerRow * centerRow + centerCol * centerCol) || 1;

  function rawDelay(row, col) {
    switch (direction) {
      case "left-to-right": return col;
      case "right-to-left": return cols - col;
      case "top-to-bottom": return row;
      case "bottom-to-top": return rows - row;
      case "center-out": return Math.sqrt((row - centerRow) ** 2 + (col - centerCol) ** 2);
      case "edges-in": return maxRadius - Math.sqrt((row - centerRow) ** 2 + (col - centerCol) ** 2);
      default: return 0;
    }
  }

  let maxRaw = 0;
  for (const t of targets) {
    const r = rawDelay(t.seatRow, t.seatCol);
    if (r > maxRaw) maxRaw = r;
  }
  if (maxRaw <= 0) maxRaw = 1;

  for (const t of targets) {
    const r = rawDelay(t.seatRow, t.seatCol);
    const offsetMs = (r / maxRaw) * totalDurationMs;
    const startAt = now + leadMs + offsetMs;
    try {
      t.send(JSON.stringify({
        type: "color",
        id: "seatwave_" + now + "_" + t.seatRow + "_" + t.seatCol,
        startAt,
        color,
        torchMode: false,
        calibration: msg.calibration || {}
      }));
    } catch (e) {}
  }

  // Dalga tüm salonu geçtikten kısa bir süre sonra herkes birlikte sönsün.
  const holdMs = Number(msg.holdMs || 400);
  setTimeout(() => {
    for (const t of targets) {
      if (t.readyState === WebSocket.OPEN) {
        try { t.send(JSON.stringify({ type: "stop", startAt: Date.now() })); } catch (e) {}
      }
    }
  }, leadMs + totalDurationMs + holdMs);

  return targets.length;
}

function createColorCommand(msg) {
  const now = Date.now();
  return {
    type: "color",
    id: "cmd_" + now + "_" + Math.random().toString(16).slice(2),
    startAt: now + Number(msg.leadMs || 180),
    color: String(msg.color || "#ffffff"),
    torchMode: !!msg.torchMode,
    calibration: {
      ios: Number(msg.calibration?.ios || 0),
      android: Number(msg.calibration?.android || 0),
      default: Number(msg.calibration?.default || 0)
    }
  };
}

function createCountdownCommand(msg) {
  const now = Date.now();
  const seconds = Math.max(1, Math.min(120, Number(msg.seconds || 5)));
  return {
    type: "countdown",
    id: "cmd_" + now + "_" + Math.random().toString(16).slice(2),
    seconds,
    startAt: now + Number(msg.leadMs || 300)
  };
}

/* ---------------- ÇEKİLİŞ (RAFFLE) ----------------
   Sunucu otoritesi: hangi telefonların hâlâ oyunda olduğunu tutar,
   rastgele aralıklarla tek tek eler, son kalanı kazanan ilan eder. */
const raffleState = new Map(); // eventCode -> { active, remaining:Set<ws>, timer, intervalMs, finalIntervalMs }
const crowdMapState = new Map(); // eventCode -> { round, targetGroup, votedCount }

function getPhoneClients(code) {
  const room = rooms.get(normalizeCode(code));
  if (!room) return [];
  return [...room].filter((c) => c.role === "phone" && c.readyState === WebSocket.OPEN);
}

function raffleBroadcastUpdate(code, extra = {}) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;
  const state = raffleState.get(eventCode);
  const payload = JSON.stringify({
    type: "raffle_update",
    eventCode,
    remaining: state ? state.remaining.size : 0,
    active: state ? state.active : false,
    serverTime: Date.now(),
    ...extra
  });
  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function stopRaffle(code) {
  const eventCode = normalizeCode(code);
  const state = raffleState.get(eventCode);
  if (state && state.timer) clearTimeout(state.timer);
  raffleState.delete(eventCode);
}

function scheduleNextElimination(code) {
  const eventCode = normalizeCode(code);
  const state = raffleState.get(eventCode);
  if (!state || !state.active) return;

  if (state.remaining.size <= 1) {
    finishRaffle(eventCode);
    return;
  }

  const inFinalStretch = state.remaining.size <= 5;
  const delay = inFinalStretch ? state.finalIntervalMs : state.intervalMs;

  state.timer = setTimeout(() => {
    const current = raffleState.get(eventCode);
    if (!current || !current.active) return;

    if (current.remaining.size <= 1) { finishRaffle(eventCode); return; }

    const arr = [...current.remaining];

    // Kalabalık büyükken (5'ten fazla kişi kaldıysa) her turda tek kişi değil,
    // kalanların yaklaşık yarısını aynı anda eleyerek hızlı daralma sağlıyoruz.
    // Aksi halde 2000 kişide tek-tek eleme ~40 dakika sürerdi.
    let eliminateCount = 1;
    if (current.remaining.size > 5) {
      eliminateCount = Math.max(1, Math.floor(current.remaining.size * 0.5));
      eliminateCount = Math.min(eliminateCount, current.remaining.size - 5);
      if (eliminateCount < 1) eliminateCount = 1;
    }

    for (let i = 0; i < eliminateCount; i++) {
      const idx = Math.floor(Math.random() * arr.length);
      const pick = arr.splice(idx, 1)[0];
      current.remaining.delete(pick);

      if (pick && pick.readyState === WebSocket.OPEN) {
        pick.send(JSON.stringify({ type: "raffle_eliminate", eventCode, serverTime: Date.now() }));
      }
    }

    raffleBroadcastUpdate(eventCode);
    scheduleNextElimination(eventCode);
  }, delay);
}

function raffleRegBroadcastUpdate(code) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;
  const state = raffleState.get(eventCode);
  const payload = JSON.stringify({
    type: "raffle_reg_update",
    eventCode,
    registered: state ? state.registrants.size : 0,
    serverTime: Date.now()
  });
  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

// Çekiliş öncesi rumuz toplama aşamasını açar — herkesin ekranında rumuz girme
// kutusu belirir. Bu aşamada henüz eleme başlamaz.
function openRaffleRegistration(code) {
  const eventCode = normalizeCode(code);
  stopRaffle(eventCode); // varsa önceki turu temizle

  const state = {
    phase: "registering", // registering -> eliminating -> (silinir)
    registrants: new Map(), // ws -> rumuz (SADECE bellekte, hiçbir yere kaydedilmez)
    remaining: new Set(),
    active: false,
    timer: null,
    intervalMs: 1200,
    finalIntervalMs: 2500
  };
  raffleState.set(eventCode, state);

  roomBroadcast(eventCode, { type: "raffle_register_start", eventCode, serverTime: Date.now() }, null);
  raffleRegBroadcastUpdate(eventCode);
  return true;
}

function startRaffle(code, opts) {
  const eventCode = normalizeCode(code);
  const state = raffleState.get(eventCode);

  // Rumuz kayıt aşaması hiç açılmadıysa (ör. eski davranışa geri dönülürse),
  // en azından o an bağlı telefonlarla devam et — sistem asla kilitlenmesin.
  const registrants = (state && state.phase === "registering")
    ? state.registrants
    : new Map(getPhoneClients(eventCode).map((c) => [c, null]));

  const remaining = new Set(registrants.keys());

  const newState = {
    phase: "eliminating",
    registrants,
    active: true,
    remaining,
    timer: null,
    intervalMs: Math.max(300, Number(opts.intervalMs || 1200)),
    finalIntervalMs: Math.max(500, Number(opts.finalIntervalMs || 2500))
  };
  raffleState.set(eventCode, newState);

  // Kayıt olmayanlar da dahil herkese "flaşını kaldır" komutu — ekranlar normale döner,
  // sadece kayıtlı olanlar eleme akışına girer.
  roomBroadcast(eventCode, { type: "raffle_start", id: "raffle_" + Date.now(), startAt: Date.now() + 250 }, null);
  raffleBroadcastUpdate(eventCode);

  if (remaining.size <= 1) {
    finishRaffle(eventCode);
  } else {
    scheduleNextElimination(eventCode);
  }
  return remaining.size;
}

function finishRaffle(code) {
  const eventCode = normalizeCode(code);
  const state = raffleState.get(eventCode);
  if (!state) return;
  state.active = false;
  if (state.timer) clearTimeout(state.timer);

  const remainingArr = [...state.remaining];
  const winner = remainingArr.length
    ? remainingArr[Math.floor(Math.random() * remainingArr.length)]
    : null;
  const winnerNickname = winner ? (state.registrants.get(winner) || null) : null;

  if (winner && winner.readyState === WebSocket.OPEN) {
    winner.send(JSON.stringify({ type: "raffle_winner", eventCode, serverTime: Date.now() }));
  }

  // "Bitir" ile erken sonlandırıldığında hâlâ oyunda olan ama kazanmayan herkese de haber ver —
  // yoksa flaşlarını kaldırmış şekilde sonsuza kadar beklerler.
  for (const client of remainingArr) {
    if (client !== winner && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: "raffle_eliminate", eventCode, serverTime: Date.now() }));
    }
  }

  raffleBroadcastUpdate(eventCode, { finished: true, hasWinner: !!winner, winnerNickname });
  raffleState.delete(eventCode);
}
/* ---------------- /ÇEKİLİŞ ---------------- */

/* ---------------- SEYİRCİ HARİTALA (kalibrasyonla 4 bölge) ---------------- */
// Hiçbir koltuk numarası önceden bilinmeden — sunucu fiziksel olarak sırayla
// farklı merkez noktalarına geçip "sağ mısın sol musun" diye sorarak seyirciyi
// A-Sol / A-Sağ / B-Sol / B-Sağ olmak üzere 4 bölgeye ayırır.
// Grup bilgisi SADECE bağlantı canlıyken bellekte tutulur, hiçbir yere kaydedilmez.

function crowdMapBroadcastStatus(code) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return;
  const state = crowdMapState.get(eventCode) || null;

  let leftCount = 0, rightCount = 0;
  let quadCounts = { "B_SOL": 0, "B_SAG": 0, "A_SOL": 0, "A_SAG": 0 };
  for (const client of room) {
    if (client.role !== "phone") continue;
    if (client.crowdQuadrant) quadCounts[client.crowdQuadrant] = (quadCounts[client.crowdQuadrant] || 0) + 1;
    if (state && state.round === 1 && client.crowdGroup1) {
      if (client.crowdGroup1 === "B") leftCount++; else rightCount++;
    }
    if (state && state.round > 1 && client.crowdGroup1 === state.targetGroup && client.crowdGroup2) {
      if (client.crowdGroup2 === "SOL") leftCount++; else rightCount++;
    }
  }

  const payload = JSON.stringify({
    type: "crowd_map_status",
    eventCode,
    round: state ? state.round : 0,
    targetGroup: state ? state.targetGroup : null,
    leftCount,
    rightCount,
    quadCounts,
    serverTime: Date.now()
  });
  for (const client of room) {
    if (client.role === "panel" && client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

// round: 1 = herkes (A/B ayrımı), 2 = sadece A grubu (A-Sol/A-Sağ), 3 = sadece B grubu (B-Sol/B-Sağ)
function startCrowdMapRound(code, round) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return 0;

  // Görsel sırayla tutarlı olsun diye: 2. Tur = B (sol), 3. Tur = A (sağ).
  const targetGroup = round === 2 ? "B" : round === 3 ? "A" : null;
  crowdMapState.set(eventCode, { round, targetGroup });

  let sentTo = 0;
  for (const client of room) {
    if (client.role !== "phone" || client.readyState !== WebSocket.OPEN) continue;
    const eligible = round === 1 || client.crowdGroup1 === targetGroup;
    if (!eligible) continue;
    try {
      client.send(JSON.stringify({ type: "crowd_map_round", eventCode, round, serverTime: Date.now() }));
      sentTo++;
    } catch (e) {}
  }
  crowdMapBroadcastStatus(eventCode);
  return sentTo;
}

function crowdMapVote(ws, choice) {
  const eventCode = ws.eventCode;
  const state = crowdMapState.get(eventCode);
  if (!state) return;

  if (state.round === 1) {
    // Görseldeki yerleşime göre: SOL = B Grubu (mavi), SAĞ = A Grubu (kırmızı).
    ws.crowdGroup1 = choice === "left" ? "B" : "A";
  } else if (state.round === 2 && ws.crowdGroup1 === "B") {
    ws.crowdGroup2 = choice === "left" ? "SOL" : "SAG";
    ws.crowdQuadrant = "B_" + ws.crowdGroup2;
  } else if (state.round === 3 && ws.crowdGroup1 === "A") {
    ws.crowdGroup2 = choice === "left" ? "SOL" : "SAG";
    ws.crowdQuadrant = "A_" + ws.crowdGroup2;
  } else {
    return; // bu turda oy kullanmaya uygun değil
  }

  ws.send(JSON.stringify({ type: "crowd_map_voted", eventCode, serverTime: Date.now() }));
  crowdMapBroadcastStatus(eventCode);
}

// Belirli bir bölgeye (A-Sol / A-Sağ / B-Sol / B-Sağ) rengi gönderir.
function broadcastToQuadrant(code, quadrant, msg) {
  const eventCode = normalizeCode(code);
  const room = rooms.get(eventCode);
  if (!room) return 0;
  let sent = 0;
  for (const client of room) {
    if (client.role === "phone" && client.readyState === WebSocket.OPEN && client.crowdQuadrant === quadrant) {
      try { client.send(JSON.stringify(msg)); sent++; } catch (e) {}
    }
  }
  return sent;
}
/* ---------------- /SEYİRCİ HARİTALA ---------------- */

/* ---------------- 4 BÖLGELİ ÖRÜNTÜ (B-Sol/B-Sağ/A-Sol/A-Sağ) ---------------- */
const quadrantPatternState = new Map(); // eventCode -> { timers: [], loop, steps }
const QUADRANTS = ["B_SOL", "B_SAG", "A_SOL", "A_SAG"]; // ASCII güvenli iç kimlikler (Türkçe karakter kodlama riskini önler)

function stopQuadrantPattern(code) {
  const eventCode = normalizeCode(code);
  const state = quadrantPatternState.get(eventCode);
  if (state) {
    state.timers.forEach((t) => clearTimeout(t));
    quadrantPatternState.delete(eventCode);
  }
  // Ekranı temiz bırak
  QUADRANTS.forEach((q) => broadcastToQuadrant(eventCode, q, { type: "stop", startAt: Date.now() }));
}

// steps: [{ durationMs, mode: 'screen'|'torch'|'both', colors: { "B_SOL": "#fff"|null, "B_SAG": ..., "A_SOL": ..., "A_SAG": ... } }, ...]
function playQuadrantPattern(code, steps, loop) {
  const eventCode = normalizeCode(code);
  stopQuadrantPattern(eventCode);

  const state = { timers: [], loop: !!loop, steps };
  quadrantPatternState.set(eventCode, state);

  function scheduleStep(index, baseDelay) {
    const step = steps[index];
    if (!step) return baseDelay;

    const torchMode = step.mode === "torch" || step.mode === "both";
    // Örüntünün kayıtlı zaman çizelgesini aynen koru. OPPO/ColorOS gibi yavaş
    // torch cihazlarında uyumluluk telefon istemcisinde yönetilir; sunucu tüm
    // cihazları 450 ms'ye zorlayarak iPhone/Samsung hızını düşürmez.
    const effectiveDurationMs = Math.max(50, Number(step.durationMs || 300));

    const timer = setTimeout(() => {
      if (!quadrantPatternState.has(eventCode)) return; // durdurulmuş

      // Dört bölgenin aynı fiziksel anda değişmesi için mesajları biraz daha erken
      // gönderip ortak bir gelecek startAt zamanı veriyoruz. 60 ms OPPO/ColorOS gibi
      // jitter'lı cihazlarda yetersiz kalabiliyordu; 260 ms Wi-Fi + browser scheduler
      // farklarını absorbe eder. Telefon istemcisi kendi clock offset'ini ölçerek bu
      // server zamanını yerel zamana çevirir.
      const targetAt = Date.now() + 260;

      QUADRANTS.forEach((q) => {
        const color = step.colors && step.colors[q];
        if (color) {
          broadcastToQuadrant(eventCode, q, {
            type: "color", id: "qp_" + Date.now() + "_" + q,
            startAt: targetAt, color, torchMode, calibration: step.calibration || {}
          });
        } else {
          broadcastToQuadrant(eventCode, q, { type: "stop", startAt: targetAt });
        }
      });
    }, baseDelay);
    state.timers.push(timer);

    return baseDelay + effectiveDurationMs;
  }

  function playOnce(startDelay) {
    let delay = startDelay;
    steps.forEach((_, i) => { delay = scheduleStep(i, delay); });
    return delay;
  }

  const totalMs = playOnce(0);

  if (loop) {
    const loopTimer = setInterval(() => {
      if (!quadrantPatternState.has(eventCode)) { clearInterval(loopTimer); return; }
      playOnce(0);
    }, totalMs);
    state.timers.push(loopTimer);

    // Güvenlik: panel kapanır/unutulursa loop sonsuza kadar çalışmasın —
    // 15 dakika sonra otomatik durur.
    const safetyTimer = setTimeout(() => stopQuadrantPattern(eventCode), 15 * 60 * 1000);
    state.timers.push(safetyTimer);
  }
}
/* ---------------- /4 BÖLGELİ ÖRÜNTÜ ---------------- */

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

    if (type === "crowd_map_vote") {
      crowdMapVote(ws, msg.choice === "left" ? "left" : "right");
      return;
    }

    if (type === "raffle_nickname_submit") {
      const eventCode = ws.eventCode || normalizeCode(msg.eventCode);
      const state = raffleState.get(eventCode);
      if (state && state.phase === "registering") {
        // Rumuz sadece bu çekiliş turu boyunca bellekte tutulur, hiçbir yere kaydedilmez.
        let nickname = String(msg.nickname || "").trim().slice(0, 24);
        if (!nickname) nickname = "İsimsiz";
        state.registrants.set(ws, nickname);
        ws.send(JSON.stringify({ type: "raffle_registered", eventCode, serverTime: Date.now() }));
        raffleRegBroadcastUpdate(eventCode);
      }
      return;
    }

    if (type === "panel_join") {
      const eventCode = normalizeCode(msg.eventCode);
      if (!eventCode) {
        ws.send(JSON.stringify({ type: "error", message: "Event code missing" }));
        return;
      }
      if (msg.secret !== SECRET) {
        ws.send(JSON.stringify({ type: "error", message: "Unauthorized panel" }));
        return;
      }

      // Panel bağlandığında etkinliğin katılımcı sınırını (Demo: 25, Pro: sınırsız)
      // otoriter şekilde güncelliyoruz — bu değer PHP tarafında veritabanından
      // hesaplanıp panele iletildiği için güvenilir kaynak burasıdır.
      roomCaps.set(eventCode, msg.maxParticipants === null || msg.maxParticipants === undefined ? null : Number(msg.maxParticipants));

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

      // Panel henüz hiç bağlanmadıysa, ilk gelen telefonun bildirdiği sınırı
      // geçici olarak benimse (panel bağlanınca gerçek/doğrulanmış değerle güncellenir).
      if (!roomCaps.has(eventCode)) {
        roomCaps.set(eventCode, msg.maxParticipants === null || msg.maxParticipants === undefined ? null : Number(msg.maxParticipants));
      }

      const cap = roomCaps.get(eventCode);
      if (typeof cap === "number" && !Number.isNaN(cap)) {
        const currentPhoneCount = getRoomPhoneOnline(eventCode);
        if (currentPhoneCount >= cap) {
          ws.send(JSON.stringify({
            type: "error",
            code: "capacity_full",
            message: "Bu etkinlik Demo modunda — en fazla " + cap + " katılımcı bağlanabilir. Pro'ya geçmek için etkinlik sahibiyle iletişime geç."
          }));
          try { ws.close(); } catch (e) {}
          return;
        }
      }

      attachClient(ws, eventCode, "phone");
      ws.deviceToken = msg.deviceToken || ("anon_" + Math.random().toString(16).slice(2));
      const st = getRoomStats(eventCode);
      if (!seenDevices.has(eventCode)) seenDevices.set(eventCode, new Set());
      const devices = seenDevices.get(eventCode);
      if (devices.has(ws.deviceToken)) st.reconnects++; else { devices.add(ws.deviceToken); st.joined++; }

      ws.ua = msg.ua || "";

      // Koltuk numarası SADECE bu bağlantı canlıyken bellekte tutulur, hiçbir yere
      // kaydedilmez — bağlantı kopunca kaybolur. Işık dalgası efektleri için gerekli.
      if (Number.isFinite(msg.seatRow) && Number.isFinite(msg.seatCol)) {
        ws.seatRow = Number(msg.seatRow);
        ws.seatCol = Number(msg.seatCol);
      }

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

      // --- Çekiliş: normal broadcast akışının dışında, kendi mantığı var ---
      if (msg.command === "raffle_open_registration") {
        openRaffleRegistration(eventCode);
        ws.send(JSON.stringify({
          type: "sent", sent: 0, eventCode,
          commandType: "raffle_open_registration", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }
      if (msg.command === "raffle_start") {
        const remaining = startRaffle(eventCode, {
          intervalMs: msg.intervalMs,
          finalIntervalMs: msg.finalIntervalMs
        });
        ws.send(JSON.stringify({
          type: "sent", sent: remaining, eventCode,
          commandType: "raffle_start", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }
      if (msg.command === "raffle_stop") {
        stopRaffle(eventCode);
        roomBroadcast(eventCode, { type: "raffle_cancelled", serverTime: Date.now() }, null);
        raffleBroadcastUpdate(eventCode);
        ws.send(JSON.stringify({
          type: "sent", sent: 0, eventCode,
          commandType: "raffle_stop", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }
      if (msg.command === "raffle_finish") {
        const stateBefore = raffleState.get(eventCode);
        const remainingCount = stateBefore ? stateBefore.remaining.size : 0;
        finishRaffle(eventCode); // kalanlar arasından rastgele kazanan seçip düzgün bitirir
        ws.send(JSON.stringify({
          type: "sent", sent: remainingCount, eventCode,
          commandType: "raffle_finish", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "seat_wave") {
        const sent = broadcastSeatWave(eventCode, msg);
        ws.send(JSON.stringify({
          type: "sent", sent, eventCode,
          commandType: "seat_wave", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "crowd_map_start_round") {
        const sent = startCrowdMapRound(eventCode, Number(msg.round || 1));
        ws.send(JSON.stringify({
          type: "sent", sent, eventCode,
          commandType: "crowd_map_start_round", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "crowd_map_quadrant_color") {
        const color = String(msg.color || "#ffffff");
        const durationMs = Math.max(300, Number(msg.durationMs || 2000));
        const sent = broadcastToQuadrant(eventCode, msg.quadrant, {
          type: "color",
          id: "crowdmap_" + Date.now(),
          startAt: Date.now() + 250,
          color,
          torchMode: false,
          calibration: msg.calibration || {}
        });
        setTimeout(() => {
          broadcastToQuadrant(eventCode, msg.quadrant, { type: "stop", startAt: Date.now() });
        }, 250 + durationMs);
        ws.send(JSON.stringify({
          type: "sent", sent, eventCode,
          commandType: "crowd_map_quadrant_color", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "crowd_map_wave") {
        const color = String(msg.color || "#ffffff");
        const stepMs = Math.max(300, Number(msg.stepMs || 700));
        const holdMs = Math.max(300, Number(msg.holdMs || 900));
        const order = ["B_SOL", "B_SAG", "A_SAG", "A_SOL"]; // soldan (B) sağa (A) fiziksel bir tur
        order.forEach((quadrant, i) => {
          setTimeout(() => {
            broadcastToQuadrant(eventCode, quadrant, {
              type: "color", id: "crowdmapwave_" + Date.now() + "_" + i,
              startAt: Date.now() + 100, color, torchMode: false, calibration: msg.calibration || {}
            });
            setTimeout(() => {
              broadcastToQuadrant(eventCode, quadrant, { type: "stop", startAt: Date.now() });
            }, 100 + holdMs);
          }, i * stepMs);
        });
        ws.send(JSON.stringify({
          type: "sent", sent: 0, eventCode,
          commandType: "crowd_map_wave", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "quadrant_pattern_play") {
        const steps = Array.isArray(msg.steps) ? msg.steps : [];
        playQuadrantPattern(eventCode, steps, !!msg.loop);
        ws.send(JSON.stringify({
          type: "sent", sent: 0, eventCode,
          commandType: "quadrant_pattern_play", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      if (msg.command === "quadrant_pattern_stop") {
        stopQuadrantPattern(eventCode);
        ws.send(JSON.stringify({
          type: "sent", sent: 0, eventCode,
          commandType: "quadrant_pattern_stop", serverTime: Date.now(), broadcastDurationMs: 0
        }));
        return;
      }

      let command;
      if (msg.command === "stop_loop") {
        command = createStopCommand(msg);
        stopRaffle(eventCode); // acil kapat, çekilişi de iptal etsin
        raffleBroadcastUpdate(eventCode, { finished: true, hasWinner: false });
      } else if (msg.command === "flash_test") {
        command = createFlashTestCommand(msg);
      } else if (msg.command === "set_color") {
        command = createColorCommand(msg);
      } else if (msg.command === "countdown") {
        command = createCountdownCommand(msg);
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

      // Kopan telefon, çekilişte hâlâ kayıtlı/yarışıyor görünüp yanlışlıkla
      // kazanan seçilmesin diye ilgili listelerden de çıkarılır.
      const raffle = raffleState.get(eventCode);
      if (raffle) {
        let changed = false;
        if (raffle.registrants && raffle.registrants.delete(ws)) changed = true;
        if (raffle.remaining && raffle.remaining.delete(ws)) changed = true;
        if (changed) {
          if (raffle.phase === "registering") raffleRegBroadcastUpdate(eventCode);
          else raffleBroadcastUpdate(eventCode);
        }
      }

      // Seyirci haritalamada da kopan telefon anlık sayımlardan düşsün.
      if (crowdMapState.has(eventCode)) {
        crowdMapBroadcastStatus(eventCode);
      }
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
