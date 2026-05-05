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
    });
  }
  return rooms.get(roomId);
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

  socket.on('join', ({ room: r, name: n }) => {
    roomId = String(r || 'main').trim() || 'main';
    name = String(n || 'Guest').slice(0, 32) || 'Guest';
    socket.join(roomId);
    const room = getRoom(roomId);
    room.listeners.set(socket.id, name);
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
    if (room.currentIdx + 1 < room.queue.length) {
      room.currentIdx += 1;
      room.position = 0;
      room.isPlaying = true;
      room.updatedAt = Date.now();
      broadcast(roomId);
    }
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
    if (room.currentIdx + 1 < room.queue.length) {
      room.currentIdx += 1;
      room.position = 0;
      room.isPlaying = true;
    } else {
      room.isPlaying = false;
      room.position = 0;
    }
    room.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('chat', ({ text }) => {
    if (!roomId) return;
    const msg = String(text || '').slice(0, 500).trim();
    if (!msg) return;
    io.to(roomId).emit('chat', { name, text: msg, at: Date.now() });
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
