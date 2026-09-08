// Peak Eleven — minimal WebSocket relay server for room-code multiplayer.
//
// This server does NOT know anything about football, drafts, or matches.
// It only does three things:
//   1. Lets a "host" create a room (with a capacity of 2-4 players) and
//      get a short room code back.
//   2. Lets up to (capacity - 1) "guests" join that room using the code,
//      each getting a playerIndex (host is always 0, guests are 1, 2, 3
//      in the order they joined).
//   3. Relays messages: anything the HOST sends goes out to every guest;
//      anything a GUEST sends goes only to the host. Guests never talk
//      to each other directly — the host is the single source of truth.
//
// All the actual game logic still runs entirely in the HOST's browser
// (the existing single-player / N-team engine, untouched). Every guest's
// browser forwards its clicks/inputs to the host over this relay, and the
// host broadcasts its resulting game state back down to everyone. This
// server is just the pipe in the middle — deliberately dumb, so it can't
// get out of sync with the game rules.

const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const MAX_CAPACITY = 4;

// rooms: code -> { host: ws|null, guests: (ws|null)[], capacity: number }
// guests[] is indexed by (playerIndex - 1), so guests[0] is playerIndex 1, etc.
const rooms = new Map();

function makeCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I to avoid confusion
  let code;
  do {
    code = Array.from({ length: 5 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastFromHost(room, msg) {
  room.guests.forEach((g) => send(g, msg));
}

function roomIsEmpty(room) {
  return !room.host && room.guests.every((g) => !g);
}

function cleanupSocket(ws) {
  if (!ws.roomCode) return;
  const room = rooms.get(ws.roomCode);
  if (!room) return;

  if (room.host === ws) {
    room.host = null;
    // Without a host nothing can run — tell every connected guest.
    room.guests.forEach((g) => send(g, { type: 'host-left' }));
  } else {
    const idx = room.guests.indexOf(ws);
    if (idx >= 0) {
      room.guests[idx] = null;
      send(room.host, { type: 'peer-left', playerIndex: idx + 1 });
    }
  }
  if (roomIsEmpty(room)) rooms.delete(ws.roomCode);
}

const server = http.createServer((req, res) => {
  // Simple health check endpoint so Render's health checks (and you) can
  // confirm the service is alive without opening a WebSocket.
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('peak-eleven relay ok, rooms: ' + rooms.size);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'create-room') {
      const capacity = Math.max(2, Math.min(MAX_CAPACITY, Number(msg.capacity) || 2));
      const code = makeCode();
      ws.roomCode = code;
      ws.playerIndex = 0;
      rooms.set(code, { host: ws, guests: new Array(capacity - 1).fill(null), capacity });
      send(ws, { type: 'room-created', code, capacity });
      return;
    }

    if (msg.type === 'join-room') {
      const code = (msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room || !room.host) {
        send(ws, { type: 'join-failed', reason: 'room-not-found' });
        return;
      }
      const freeSlot = room.guests.indexOf(null);
      if (freeSlot === -1) {
        send(ws, { type: 'join-failed', reason: 'room-full' });
        return;
      }
      room.guests[freeSlot] = ws;
      ws.roomCode = code;
      const playerIndex = freeSlot + 1;
      ws.playerIndex = playerIndex;
      send(ws, { type: 'joined', code, playerIndex, capacity: room.capacity });
      send(room.host, { type: 'peer-joined', name: msg.name || '', playerIndex });
      return;
    }

    // Anything else: relay per the host/guest rule described up top.
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    if (room.host === ws) {
      broadcastFromHost(room, msg);
    } else if (room.guests.includes(ws)) {
      send(room.host, msg);
    }
  });

  ws.on('close', () => cleanupSocket(ws));
  ws.on('error', () => cleanupSocket(ws));
});

// Keep connections alive through Render's proxy and drop dead ones.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log('Peak Eleven relay server listening on port ' + PORT);
});
