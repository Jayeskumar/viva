const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

function decodeEntities(s) {
  return String(s || '')
    .replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
}

function parseDuration(s) {
  if (!s) return 0;
  const parts = String(s).split(':').map((x) => parseInt(x, 10) || 0);
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  return parts[0] || 0;
}

app.get('/api/yt-search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) return res.json({ results: [] });
  try {
    const url = `https://www.youtube.com/results?search_query=${encodeURIComponent(q)}&sp=EgIQAQ%253D%253D`;
    const r = await fetch(url, {
      headers: {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'accept-language': 'en-US,en;q=0.9',
      },
    });
    if (!r.ok) return res.status(502).json({ error: `upstream ${r.status}` });
    const html = await r.text();
    const m = html.match(/var ytInitialData = (\{[\s\S]*?\});<\/script>/);
    if (!m) return res.json({ results: [] });
    const data = JSON.parse(m[1]);
    const sections = data?.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents || [];
    const results = [];
    for (const sec of sections) {
      const items = sec?.itemSectionRenderer?.contents || [];
      for (const it of items) {
        const v = it.videoRenderer;
        if (!v || !v.videoId) continue;
        const title = decodeEntities(v.title?.runs?.[0]?.text || '');
        const channel = decodeEntities(v.ownerText?.runs?.[0]?.text || v.longBylineText?.runs?.[0]?.text || '');
        const dur = parseDuration(v.lengthText?.simpleText || '');
        const thumbs = v.thumbnail?.thumbnails || [];
        const thumb = thumbs[thumbs.length - 1]?.url || `https://i.ytimg.com/vi/${v.videoId}/mqdefault.jpg`;
        if (title) results.push({ videoId: v.videoId, title, artist: channel, duration: dur, image: thumb });
        if (results.length >= 20) break;
      }
      if (results.length >= 20) break;
    }
    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: 'search failed', message: e.message });
  }
});

const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      queue: [],
      currentIdx: -1,
      isPlaying: false,
      position: 0,
      updatedAt: Date.now(),
      listeners: new Map(),
      shuffle: false,
      repeat: 'off', // 'off' | 'one' | 'all'
      played: new Set(),
    });
  }
  return rooms.get(roomId);
}

function pickRandomNext(room) {
  const remaining = room.queue.filter((t) => !room.played.has(t.id));
  if (remaining.length > 0) {
    const next = remaining[Math.floor(Math.random() * remaining.length)];
    return room.queue.findIndex((t) => t.id === next.id);
  }
  if (room.repeat === 'all' && room.queue.length > 0) {
    room.played = new Set();
    const next = room.queue[Math.floor(Math.random() * room.queue.length)];
    return room.queue.findIndex((t) => t.id === next.id);
  }
  return -1;
}

function currentPosition(room) {
  if (!room.isPlaying) return room.position;
  return room.position + (Date.now() - room.updatedAt) / 1000;
}

function snapshot(room) {
  return {
    queue: room.queue,
    currentIdx: room.currentIdx,
    isPlaying: room.isPlaying,
    position: currentPosition(room),
    serverTime: Date.now(),
    listeners: Array.from(room.listeners.values()),
    shuffle: room.shuffle,
    repeat: room.repeat,
  };
}

function broadcast(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  io.to(roomId).emit('state', snapshot(room));
}

io.on('connection', (socket) => {
  let roomId = null;
  let name = 'Guest';

  let avatar = '';
  socket.on('join', ({ room: r, name: n, avatar: a }) => {
    roomId = String(r || 'main').trim() || 'main';
    name = String(n || 'Guest').slice(0, 32) || 'Guest';
    avatar = String(a || '').slice(0, 500);
    socket.join(roomId);
    const room = getRoom(roomId);
    room.listeners.set(socket.id, { name, avatar });
    socket.emit('state', snapshot(room));
    broadcast(roomId);
  });

  socket.on('add', ({ videoId, title, artist, image }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const vid = String(videoId || '').trim();
    if (!vid || !/^[A-Za-z0-9_-]{11}$/.test(vid)) return;
    const trackTitle = String(title || vid).slice(0, 200) || vid;
    room.queue.push({
      videoId: vid,
      title: trackTitle,
      artist: String(artist || '').slice(0, 200),
      image: String(image || '').slice(0, 500) || `https://i.ytimg.com/vi/${vid}/mqdefault.jpg`,
      addedBy: name,
      id: Date.now() + Math.random(),
    });
    if (room.currentIdx === -1 && room.queue.length === 1) {
      room.currentIdx = 0;
      room.position = 0;
      room.isPlaying = true;
      room.updatedAt = Date.now();
    }
    broadcast(roomId);
  });

  socket.on('remove', ({ id }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const idx = room.queue.findIndex((t) => t.id === id);
    if (idx === -1) return;
    room.queue.splice(idx, 1);
    if (idx < room.currentIdx) {
      room.currentIdx -= 1;
    } else if (idx === room.currentIdx) {
      if (room.currentIdx >= room.queue.length) room.currentIdx = room.queue.length - 1;
      room.position = 0;
      room.updatedAt = Date.now();
    }
    broadcast(roomId);
  });

  socket.on('play', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    if (room.currentIdx === -1) return;
    if (!room.isPlaying) {
      room.isPlaying = true;
      room.updatedAt = Date.now();
    }
    broadcast(roomId);
  });

  socket.on('pause', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    if (room.isPlaying) {
      room.position = currentPosition(room);
      room.isPlaying = false;
      room.updatedAt = Date.now();
    }
    broadcast(roomId);
  });

  socket.on('seek', ({ position }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const pos = Math.max(0, Number(position) || 0);
    room.position = pos;
    room.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('next', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    if (room.queue.length === 0) return;
    let nextIdx = -1;
    if (room.shuffle) {
      const cur = room.queue[room.currentIdx];
      if (cur) room.played.add(cur.id);
      nextIdx = pickRandomNext(room);
    } else if (room.currentIdx + 1 < room.queue.length) {
      nextIdx = room.currentIdx + 1;
    } else if (room.repeat === 'all') {
      nextIdx = 0;
    }
    if (nextIdx === -1) return;
    room.currentIdx = nextIdx;
    if (room.shuffle) room.played.add(room.queue[nextIdx].id);
    room.position = 0;
    room.isPlaying = true;
    room.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('prev', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    if (room.currentIdx > 0) {
      room.currentIdx -= 1;
      room.position = 0;
      room.isPlaying = true;
      room.updatedAt = Date.now();
      broadcast(roomId);
    }
  });

  socket.on('jump', ({ id }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const idx = room.queue.findIndex((t) => t.id === id);
    if (idx === -1) return;
    room.currentIdx = idx;
    room.position = 0;
    room.isPlaying = true;
    room.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('ended', ({ trackId }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const cur = room.queue[room.currentIdx];
    if (!cur || cur.id !== trackId) return;
    if (room.repeat === 'one') {
      room.position = 0;
      room.isPlaying = true;
    } else if (room.shuffle) {
      room.played.add(cur.id);
      const nextIdx = pickRandomNext(room);
      if (nextIdx === -1) {
        room.isPlaying = false; room.position = 0;
      } else {
        room.currentIdx = nextIdx;
        room.played.add(room.queue[nextIdx].id);
        room.position = 0; room.isPlaying = true;
      }
    } else if (room.currentIdx + 1 < room.queue.length) {
      room.currentIdx += 1; room.position = 0; room.isPlaying = true;
    } else if (room.repeat === 'all' && room.queue.length > 0) {
      room.currentIdx = 0; room.position = 0; room.isPlaying = true;
    } else {
      room.isPlaying = false; room.position = 0;
    }
    room.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('setShuffle', ({ on }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    room.shuffle = !!on;
    room.played = new Set();
    if (room.shuffle && room.queue[room.currentIdx]) {
      room.played.add(room.queue[room.currentIdx].id);
    }
    broadcast(roomId);
  });

  socket.on('setRepeat', ({ mode }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    if (['off', 'one', 'all'].includes(mode)) room.repeat = mode;
    broadcast(roomId);
  });

  socket.on('reorder', ({ fromIdx, toIdx }) => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const f = Number(fromIdx), t = Number(toIdx);
    if (!Number.isInteger(f) || !Number.isInteger(t)) return;
    if (f === t) return;
    if (f < 0 || f >= room.queue.length || t < 0 || t >= room.queue.length) return;
    const [item] = room.queue.splice(f, 1);
    room.queue.splice(t, 0, item);
    if (room.currentIdx === f) {
      room.currentIdx = t;
    } else if (f < room.currentIdx && t >= room.currentIdx) {
      room.currentIdx -= 1;
    } else if (f > room.currentIdx && t <= room.currentIdx) {
      room.currentIdx += 1;
    }
    broadcast(roomId);
  });

  socket.on('react', ({ emoji }) => {
    if (!roomId) return;
    const e = String(emoji || '').slice(0, 8);
    if (!e) return;
    io.to(roomId).emit('reaction', { name, emoji: e, at: Date.now() });
  });

  socket.on('call-request', () => {
    if (!roomId) return;
    const room = getRoom(roomId);
    const others = [...room.listeners.keys()].filter((id) => id !== socket.id);
    if (others.length === 0) {
      socket.emit('call-error', { message: 'No one else is in this room' });
      return;
    }
    const target = others[0];
    io.to(target).emit('call-incoming', { from: socket.id, fromName: name });
  });

  socket.on('call-accept', ({ to }) => {
    if (to) io.to(to).emit('call-accepted', { from: socket.id, fromName: name });
  });

  socket.on('call-decline', ({ to }) => {
    if (to) io.to(to).emit('call-declined', { from: socket.id });
  });

  socket.on('call-cancel', ({ to }) => {
    if (to) io.to(to).emit('call-canceled', { from: socket.id });
  });

  socket.on('call-end', ({ to }) => {
    if (to) io.to(to).emit('call-ended', { from: socket.id });
  });

  socket.on('call-signal', ({ to, signal }) => {
    if (!to || !signal) return;
    io.to(to).emit('call-signal', { from: socket.id, signal });
  });

  socket.on('chat', ({ text }) => {
    if (!roomId) return;
    const msg = String(text || '').slice(0, 500).trim();
    if (!msg) return;
    io.to(roomId).emit('chat', { name, avatar, text: msg, at: Date.now() });
  });

  socket.on('scene', ({ scene }) => {
    if (!roomId) return;
    const allowed = ['hug', 'kiss', 'marry', 'dance', 'pray', 'fight', 'highfive', 'play', 'eat', 'sleep'];
    const s = String(scene || '');
    if (!allowed.includes(s)) return;
    const room = rooms.get(roomId);
    if (!room) return;
    const others = [...room.listeners.values()].filter((l) => l.name !== name);
    const target = others[0] || null;
    io.to(roomId).emit('scene', {
      scene: s,
      from: name,
      fromAvatar: avatar,
      to: target ? target.name : '',
      toAvatar: target ? target.avatar : '',
      at: Date.now(),
    });
  });

  socket.on('disconnect', () => {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    room.listeners.delete(socket.id);
    if (room.listeners.size === 0) {
      rooms.delete(roomId);
    } else {
      broadcast(roomId);
    }
  });
});

setInterval(() => {
  for (const id of rooms.keys()) broadcast(id);
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Viva server running at http://localhost:${PORT}`);
});
