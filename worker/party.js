// SODDROP party server on Cloudflare Workers.
//
// One Durable Object per room code. index.html already talks this protocol:
//   wss://<worker-host>/party/<CODE>/ws
// so no client changes are needed when the Worker also serves index.html.
//
// The DO is authoritative only for party state (members, teams, ready, phase);
// gameplay messages are relayed to the other clients, exactly like the old
// party server did.

const MAX_MEMBERS = 8;
const COUNTDOWN_MS = 3200;
const PATH_RE = /^\/party\/([A-Za-z0-9]{4,6})\/ws$/;

const freshRoom = code => ({ code, phase: 'lobby', mode: 1, seed: 0, endsAt: 0, members: [] });
// Same sanitizer the client uses on names.
const cleanName = v => String(v == null ? '' : v).replace(/[^\w \-.]/g, '').trim().slice(0, 12);
const num = v => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(-10000, Math.min(10000, n)) : 0;
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(PATH_RE);
    if (!match) return new Response('Not found', { status: 404 });
    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return new Response('This endpoint only accepts WebSocket upgrades.', { status: 426 });
    }
    const code = match[1].toUpperCase();
    return env.PARTY.get(env.PARTY.idFromName('room:' + code)).fetch(request);
  },
};

export class PartyRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.room = null;
    this.timer = null;
    // Hibernation drops memory: reload the room and its sockets, and re-arm a
    // countdown that was running when the DO went to sleep.
    ctx.blockConcurrencyWhile(async () => {
      this.room = (await ctx.storage.get('room')) || null;
      if (!this.room) return;
      if (this.reconcile()) await this.save();
      if (this.room.phase === 'countdown') this.armCountdown();
    });
  }

  // ---------- connection ----------
  async fetch(request) {
    const url = new URL(request.url);
    const code = (url.pathname.match(PATH_RE) || [])[1] || '';
    if (!this.room) this.room = freshRoom(code.toUpperCase());

    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);

    if (this.room.members.length >= MAX_MEMBERS) {
      server.send(JSON.stringify({ t: 'notice', msg: 'That party is full (' + MAX_MEMBERS + ' players max).' }));
      server.close(4001, 'party full');
      return new Response(null, { status: 101, webSocket: client });
    }

    const member = {
      id: crypto.randomUUID(),
      name: 'Sod' + (100 + Math.floor(Math.random() * 900)),
      team: 1,
      ready: false,
      host: this.room.members.length === 0,
      alive: true,
    };
    this.room.members.push(member);
    this.assignTeams();
    server.serializeAttachment({ id: member.id });
    await this.save();

    this.send(server, {
      t: 'welcome',
      id: member.id,
      code: this.room.code,
      phase: this.room.phase,
      mode: this.room.mode,
      endsAt: this.room.endsAt,
      members: this.room.members,
    });
    this.broadcastRoster(member.id);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 8192) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    const me = this.memberFor(ws);
    if (!me) return;

    // A countdown can outlive the in-memory timer after hibernation.
    if (this.room.phase === 'countdown' && Date.now() >= this.room.endsAt) await this.beginMatch();

    try {
      switch (msg.t) {
        case 'hello':
        case 'name': {
          const name = cleanName(msg.name);
          if (name && name !== me.name) {
            me.name = name;
            await this.save();
            this.broadcastRoster();
          }
          break;
        }

        case 'ready': {
          if (this.room.phase === 'playing') break;
          me.ready = !!msg.v;
          if (this.room.members.length && this.room.members.every(m => m.ready)) {
            await this.beginCountdown();
          } else {
            if (this.room.phase === 'countdown') {
              this.room.phase = 'lobby';
              this.room.endsAt = 0;
              this.clearTimer();
            }
            await this.save();
            this.broadcastRoster();
          }
          break;
        }

        case 'mode': {
          const v = Math.max(1, Math.min(4, Number(msg.v) | 0));
          if (me.host && v !== this.room.mode) {
            this.room.mode = v;
            this.assignTeams();
            await this.save();
            this.broadcastRoster();
          }
          break;
        }

        case 'tolobby': {
          this.clearTimer();
          this.room.phase = 'lobby';
          this.room.endsAt = 0;
          for (const m of this.room.members) { m.ready = false; m.alive = true; }
          await this.save();
          this.broadcast({ t: 'tolobby', by: me.name });
          this.broadcastRoster();
          break;
        }

        case 'pos': {
          me.alive = msg.alive !== false;
          // Client -> everyone else. The sender already knows where it is.
          this.broadcast({
            t: 'state',
            id: me.id,
            name: me.name,
            x: num(msg.x), y: num(msg.y), z: num(msg.z), yaw: num(msg.yaw),
            st: String(msg.st || 'ground').slice(0, 8),
            hp: num(msg.hp), sh: num(msg.sh),
            alive: me.alive,
          }, me.id);
          break;
        }

        case 'hit': {
          // PvP damage is unicast to the target: every other client would
          // otherwise take the same hit.
          const target = this.room.members.find(m => m.id === msg.target);
          if (target && target.id !== me.id) {
            this.sendToMember(target.id, {
              t: 'hit',
              dmg: Math.max(1, Math.min(45, Math.round(num(msg.dmg)))),
              head: !!msg.head,
              from: me.id,
              name: me.name,
            });
          }
          break;
        }

        case 'died': {
          if (me.alive) { me.alive = false; await this.save(); }
          this.broadcast({ t: 'died', id: me.id, name: me.name }, me.id);
          this.broadcastRoster();
          break;
        }

        // Host-authoritative bot snapshots, guest bot hits, kill announcements,
        // builds and gunshots: pure relays, just never back to the sender.
        case 'bs':
        case 'bhit':
        case 'kill':
        case 'build':
        case 'sdam':
        case 'sdel':
        case 'shot': {
          this.broadcast(msg, me.id);
          break;
        }

        default:
          break;
      }
    } catch (err) {
      // Never let one malformed message kill the socket.
      console.error('party message failed', err && err.stack ? err.stack : err);
    }
  }

  async webSocketClose(ws) { await this.dropSocket(ws); }
  async webSocketError(ws) { await this.dropSocket(ws); }

  async dropSocket(ws) {
    const id = (ws.deserializeAttachment() || {}).id;
    const i = this.room.members.findIndex(m => m.id === id);
    if (i < 0) return;
    const [gone] = this.room.members.splice(i, 1);
    if (gone.host && this.room.members.length) this.room.members[0].host = true;
    this.assignTeams();

    if (!this.room.members.length) {
      this.clearTimer();
      this.room = freshRoom(this.room.code);
      await this.save();
      return;
    }
    if (this.room.phase === 'countdown' && !this.room.members.every(m => m.ready)) {
      this.room.phase = 'lobby';
      this.room.endsAt = 0;
      this.clearTimer();
    }
    await this.save();
    this.broadcastRoster();
  }

  // ---------- match flow ----------
  async beginCountdown() {
    this.room.phase = 'countdown';
    this.room.endsAt = Date.now() + COUNTDOWN_MS;
    await this.save();
    this.broadcastRoster();
    this.broadcast({ t: 'countdown', endsAt: this.room.endsAt });
    this.armCountdown();
  }

  armCountdown() {
    this.clearTimer();
    if (this.room.phase !== 'countdown') return;
    const left = Math.max(0, this.room.endsAt - Date.now());
    this.timer = setTimeout(() => { this.beginMatch().catch(() => {}); }, left);
  }

  async beginMatch() {
    if (this.room.phase !== 'countdown') return;
    this.clearTimer();
    this.room.phase = 'playing';
    this.room.endsAt = 0;
    // Same seed on every client -> same bot roster, same zone plan, same bus.
    this.room.seed = (Math.random() * 0x7fffffff) >>> 0;
    for (const m of this.room.members) { m.alive = true; m.ready = false; }
    await this.save();
    this.broadcast({ t: 'start', seed: this.room.seed, mode: this.room.mode });
    this.broadcastRoster();
  }

  // ---------- helpers ----------
  assignTeams() {
    const size = Math.max(1, Math.min(4, this.room.mode || 1));
    this.room.members.forEach((m, i) => {
      m.team = size === 1 ? 1 : Math.min(size, Math.floor(i / size) + 1);
    });
  }

  reconcile() {
    const live = new Set(
      this.ctx.getWebSockets()
        .map(ws => (ws.deserializeAttachment() || {}).id)
        .filter(Boolean),
    );
    if (this.room.members.length === live.size) return false;
    this.room.members = this.room.members.filter(m => live.has(m.id));
    this.assignTeams();
    return true;
  }

  memberFor(ws) {
    const id = (ws.deserializeAttachment() || {}).id;
    if (!id) return null;
    return this.room.members.find(m => m.id === id) || null;
  }

  save() { return this.ctx.storage.put('room', this.room); }

  clearTimer() {
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
  }

  send(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  sendToMember(id, obj) {
    for (const ws of this.ctx.getWebSockets()) {
      if ((ws.deserializeAttachment() || {}).id === id) { this.send(ws, obj); return; }
    }
  }

  broadcast(obj, exceptId) {
    const data = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      const at = ws.deserializeAttachment() || {};
      if (exceptId && at.id === exceptId) continue;
      try { ws.send(data); } catch {}
    }
  }

  broadcastRoster(exceptId) {
    this.broadcast({
      t: 'roster',
      phase: this.room.phase,
      mode: this.room.mode,
      endsAt: this.room.endsAt,
      members: this.room.members,
    }, exceptId);
  }
}
