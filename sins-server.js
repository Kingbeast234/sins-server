/**
 * SINS-PARADOX Simple Multiplayer Server
 * ----------------------------------------
 * Run with:  node sins-server.js
 * Then open the game and use Multiplayer → Create / Join / Quick Match
 *
 * This is a lightweight WebSocket lobby + game relay.
 * It does NOT contain the full game rules — the clients still run the logic.
 * The server only keeps rooms in sync and relays actions.
 */

const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const rooms = new Map(); // code -> { players: Map(id, {name, isHost, ready}), hostId, maxPlayers, state }

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

function broadcast(room, msg, exceptId = null) {
  const data = JSON.stringify(msg);
  for (const [id, p] of room.players) {
    if (id !== exceptId && p.ws && p.ws.writable && !p.ws.destroyed) {
      try { p.ws.write(encodeFrame(data)); } catch (e) {}
    }
  }
}

function roomPublicState(room) {
  return {
    code: room.code,
    players: [...room.players.values()].map(p => ({
      id: p.id,
      name: p.name,
      isHost: p.isHost,
      ready: p.ready,
      isBot: !!p.isBot
    })),
    maxPlayers: room.maxPlayers,
    started: !!room.started
  };
}

// Very small WebSocket implementation without external deps
function parseWsKey(key) {
  return crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');
}

// Try to pull ONE complete frame off the front of `buffer`.
// Returns { text, opcode, bytesConsumed } or null if there isn't a full frame yet
// (caller should wait for more data). This replaces the old single-shot decoder,
// which assumed exactly one frame per TCP chunk and silently discarded anything
// after it — a real problem once multiple small messages (syncs, prompts, hand
// updates) can legitimately arrive coalesced in one chunk.
function tryParseFrame(buffer) {
  if (buffer.length < 2) return null;
  const second = buffer[1];
  const isMasked = (second & 0x80) !== 0;
  let len = second & 0x7f;
  let offset = 2;
  if (len === 126) {
    if (buffer.length < 4) return null;
    len = buffer.readUInt16BE(2);
    offset = 4;
  } else if (len === 127) {
    if (buffer.length < 10) return null;
    const high = buffer.readUInt32BE(2);
    const low = buffer.readUInt32BE(6);
    if (high !== 0) return null; // unsupported (>4GB), not needed for this prototype
    len = low;
    offset = 10;
  }
  const maskLen = isMasked ? 4 : 0;
  const totalLen = offset + maskLen + len;
  if (buffer.length < totalLen) return null; // frame incomplete, wait for more data

  let dataStart = offset;
  let data;
  if (isMasked) {
    const mask = buffer.slice(offset, offset + 4);
    dataStart = offset + 4;
    data = Buffer.alloc(len);
    for (let i = 0; i < len; i++) data[i] = buffer[dataStart + i] ^ mask[i % 4];
  } else {
    data = buffer.slice(dataStart, dataStart + len);
  }
  const opcode = buffer[0] & 0x0f;
  return { text: data.toString('utf8'), opcode, bytesConsumed: totalLen };
}

function encodeFrame(str) {
  const payload = Buffer.from(str);
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  }
  return Buffer.concat([header, payload]);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SINS-PARADOX Multiplayer Server is running.\nConnect the game client to this server.');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }
  const accept = parseWsKey(key);
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  let playerId = crypto.randomBytes(4).toString('hex');
  let currentRoom = null;
  let buffer = Buffer.alloc(0);

  function send(obj) {
    if (socket.writable) {
      try { socket.write(encodeFrame(JSON.stringify(obj))); } catch (e) {}
    }
  }

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (true) {
      let frame;
      try {
        frame = tryParseFrame(buffer);
      } catch (e) {
        buffer = Buffer.alloc(0); // corrupt frame — drop and resync on next chunk
        break;
      }
      if (!frame) break; // no complete frame yet, wait for more data
      buffer = buffer.slice(frame.bytesConsumed);

      if (frame.opcode === 0x8) { // close
        socket.end();
        return;
      }
      if (frame.opcode === 0x9) { // ping — reply with pong (opcode reuse of encodeFrame is fine, ignored by our simple clients)
        continue;
      }
      if (frame.opcode !== 0x1) continue; // only handle text frames

      try {
        const msg = JSON.parse(frame.text);
        handleMessage(msg);
      } catch (e) {
        // not valid JSON — ignore this frame and keep processing the rest of the buffer
      }
    }
  });

  socket.on('close', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.players.delete(playerId);
        if (room.players.size === 0) {
          rooms.delete(currentRoom);
        } else {
          // if host left, assign new host
          const stillThere = [...room.players.values()];
          if (stillThere.length && !stillThere.some(p => p.isHost)) {
            stillThere[0].isHost = true;
          }
          broadcast(room, { type: 'room_update', room: roomPublicState(room) });
        }
      }
    }
  });

  function handleMessage(msg) {
    switch (msg.type) {
      case 'create': {
        let code;
        do { code = generateCode(); } while (rooms.has(code));
        const room = {
          code,
          players: new Map(),
          maxPlayers: 3,
          started: false
        };
        room.players.set(playerId, {
          id: playerId,
          name: msg.name || 'HOST',
          isHost: true,
          ready: false,
          ws: socket,
          isBot: false
        });
        rooms.set(code, room);
        currentRoom = code;
        send({ type: 'joined', you: playerId, room: roomPublicState(room) });
        break;
      }
      case 'join': {
        const room = rooms.get((msg.code || '').toUpperCase());
        if (!room) {
          send({ type: 'error', message: 'Room not found' });
          return;
        }
        if (room.players.size >= room.maxPlayers) {
          send({ type: 'error', message: 'Room full' });
          return;
        }
        if (room.started) {
          send({ type: 'error', message: 'Game already started' });
          return;
        }
        room.players.set(playerId, {
          id: playerId,
          name: msg.name || 'PLAYER',
          isHost: false,
          ready: false,
          ws: socket,
          isBot: false
        });
        currentRoom = room.code;
        send({ type: 'joined', you: playerId, room: roomPublicState(room) });
        broadcast(room, { type: 'room_update', room: roomPublicState(room) }, playerId);
        break;
      }
      case 'add_bot': {
        const room = rooms.get(currentRoom);
        if (!room) return;
        const me = room.players.get(playerId);
        if (!me || !me.isHost) return;
        if (room.players.size >= room.maxPlayers) return;
        const botId = 'bot_' + crypto.randomBytes(3).toString('hex');
        const botNames = ['FOE A', 'FOE B', 'RIVAL', 'SINNER'];
        const used = [...room.players.values()].map(p => p.name);
        const name = botNames.find(n => !used.includes(n)) || 'BOT';
        room.players.set(botId, {
          id: botId,
          name,
          isHost: false,
          ready: true,
          isBot: true,
          ws: null
        });
        broadcast(room, { type: 'room_update', room: roomPublicState(room) });
        break;
      }
      case 'start': {
        const room = rooms.get(currentRoom);
        if (!room) return;
        const me = room.players.get(playerId);
        if (!me || !me.isHost) return;
        if (room.players.size < 2) {
          send({ type: 'error', message: 'Need at least 2 players' });
          return;
        }
        room.started = true;
        broadcast(room, { type: 'game_start', room: roomPublicState(room) });
        break;
      }
      case 'action': {
        // Relay a game action. If msg.to is set, deliver privately to just that
        // player (used for private hand contents and targeted prompts). Otherwise
        // broadcast to everyone else in the room (public state sync).
        const room = rooms.get(currentRoom);
        if (!room) return;
        if (msg.to) {
          const target = room.players.get(msg.to);
          if (target && target.ws && target.ws.writable && !target.ws.destroyed) {
            try {
              target.ws.write(encodeFrame(JSON.stringify({ type: 'action', from: playerId, action: msg.action })));
            } catch (e) {}
          }
        } else {
          broadcast(room, { type: 'action', from: playerId, action: msg.action }, playerId);
        }
        break;
      }
      case 'ping':
        send({ type: 'pong' });
        break;
    }
  }

  send({ type: 'welcome', id: playerId });
});

server.listen(PORT, () => {
  console.log(`SINS-PARADOX Multiplayer Server running on port ${PORT}`);
  console.log(`Open the game and connect to ws://localhost:${PORT}`);
});
