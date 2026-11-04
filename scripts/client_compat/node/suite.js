// ioredis / node-redis compatibility suite for scripts/client_compat/run.py.
// Usage: node suite.js ioredis|node-redis
// Runs the shared scenarios against RUDIS_PORT (standalone) and
// RUDIS_CLUSTER_PORT (cluster seed); one JSON line per scenario.
'use strict';
const which = process.argv[2];
const PORT = +process.env.RUDIS_PORT;
const CPORT = +process.env.RUDIS_CLUSTER_PORT;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const check = (cond, msg) => { if (!cond) throw new Error(msg); };
const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);

async function report(version, name, fn) {
  let ok = true, error = '';
  try { await fn(); } catch (e) { ok = false; error = String(e && e.message || e).slice(0, 300); }
  console.log(JSON.stringify({ client: which, version, scenario: name, ok, error }));
}

async function ioredisSuite() {
  const Redis = require('ioredis');
  const version = require('ioredis/package.json').version;
  const r = new Redis({ port: PORT, lazyConnect: false });
  const fresh = () => new Redis({ port: PORT });
  await report(version, 'strings', async () => {
    await r.flushall();
    check(await r.set('s', 'v', 'EX', 100) === 'OK', 'set');
    check(await r.get('s') === 'v', 'get');
    const ttl = await r.ttl('s'); check(ttl > 0 && ttl <= 100, `ttl ${ttl}`);
    check(await r.incrby('n', 6) === 6, 'incrby');
    await r.mset('a', '1', 'b', '2');
    check(eq(await r.mget('a', 'b', 'missing'), ['1', '2', null]), 'mget');
    check(await r.set('s', 'w', 'NX') === null, 'set nx');
    check(await r.get('missing') === null, 'get missing');
  });
  await report(version, 'collections', async () => {
    await r.hset('h', { f1: '1', f2: '2' });
    check(eq(await r.hgetall('h'), { f1: '1', f2: '2' }), 'hgetall');
    await r.rpush('l', 'a', 'b', 'c');
    check(eq(await r.lrange('l', 0, -1), ['a', 'b', 'c']), 'lrange');
    await r.sadd('st', 'x', 'y');
    check((await r.smembers('st')).length === 2, 'smembers');
    await r.zadd('z', 1.5, 'm1');
    check(await r.zscore('z', 'm1') === '1.5', 'zscore');
  });
  await report(version, 'pipeline', async () => {
    let p = r.pipeline();
    for (let i = 0; i < 1000; i++) p.set(`p:${i}`, i);
    await p.exec();
    p = r.pipeline();
    for (let i = 0; i < 1000; i++) p.get(`p:${i}`);
    const res = await p.exec();
    check(res.every(([err, v], i) => !err && v === String(i)), 'pipelined gets');
  });
  await report(version, 'transaction', async () => {
    await r.set('tx', '0');
    const res = await r.multi().incr('tx').incr('tx').exec();
    check(res[1][1] === 2, 'multi/exec');
    const w = fresh();
    await w.watch('tx');
    await r.set('tx', 'changed');
    const aborted = await w.multi().set('tx', 'mine').exec();
    check(aborted === null, 'watch conflict');
    w.disconnect();
  });
  await report(version, 'pubsub', async () => {
    const sub = fresh();
    const got = {};
    sub.on('message', (ch, m) => { got[ch] = m; });
    sub.on('pmessage', (p, ch, m) => { got[ch] = m; });
    await sub.subscribe('ch');
    await sub.psubscribe('pat.*');
    for (let i = 0; i < 25 && Object.keys(got).length < 2; i++) {
      await r.publish('ch', 'm1'); await r.publish('pat.x', 'm2'); await sleep(200);
    }
    sub.disconnect();
    check(got.ch === 'm1' && got['pat.x'] === 'm2', JSON.stringify(got));
  });
  await report(version, 'scripting', async () => {
    check(await r.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, 'lk', 'lv') === 'OK', 'eval');
    r.defineCommand('getk', { numberOfKeys: 1, lua: "return redis.call('GET', KEYS[1])" });
    check(await r.getk('lk') === 'lv', 'evalsha');
    await r.script('FLUSH');
    check(await r.getk('lk') === 'lv', 'evalsha after flush');
  });
  await report(version, 'blocking', async () => {
    await r.del('bq');
    const b = fresh();
    setTimeout(() => r.rpush('bq', 'x'), 200);
    check(eq(await b.blpop('bq', 5), ['bq', 'x']), 'blpop');
    b.disconnect();
  });
  await report(version, 'streams', async () => {
    await r.del('xs');
    await r.xadd('xs', '*', 'f', 'v');
    await r.xgroup('CREATE', 'xs', 'g', '0');
    const got = await r.xreadgroup('GROUP', 'g', 'c', 'COUNT', 10, 'STREAMS', 'xs', '>');
    check(got && got[0][0] === 'xs' && got[0][1].length === 1, JSON.stringify(got));
    check(await r.xack('xs', 'g', got[0][1][0][0]) === 1, 'xack');
    check((await r.xpending('xs', 'g'))[0] === 0, 'xpending');
  });
  await report(version, 'scan', async () => {
    await r.flushall();
    const p = r.pipeline();
    for (let i = 0; i < 500; i++) p.set(`scan:${i}`, 1);
    await p.exec();
    const seen = new Set();
    await new Promise((res, rej) => {
      r.scanStream({ match: 'scan:*', count: 50 })
        .on('data', (ks) => ks.forEach((k) => seen.add(k))).on('end', res).on('error', rej);
    });
    check(seen.size === 500, `scan saw ${seen.size}`);
  });
  await report(version, 'cluster', async () => {
    const rc = new Redis.Cluster([{ host: '127.0.0.1', port: CPORT }]);
    for (let i = 0; i < 200; i++) await rc.set(`ck:${i}`, i);
    for (let i = 0; i < 200; i++) check(await rc.get(`ck:${i}`) === String(i), `get ck:${i}`);
    await rc.mset('{t}a', '1', '{t}b', '2');
    check(eq(await rc.mget('{t}a', '{t}b'), ['1', '2']), 'mget');
    rc.disconnect();
  });
  r.disconnect();
}

async function nodeRedisSuite() {
  const { createClient, createCluster } = require('redis');
  const version = require('redis/package.json').version;
  const url = `redis://127.0.0.1:${PORT}`;
  const r = await createClient({ url }).connect();
  const fresh = () => createClient({ url }).connect();
  await report(version, 'strings', async () => {
    await r.flushAll();
    check(await r.set('s', 'v', { EX: 100 }) === 'OK', 'set');
    check(await r.get('s') === 'v', 'get');
    const ttl = await r.ttl('s'); check(ttl > 0 && ttl <= 100, `ttl ${ttl}`);
    check(await r.incrBy('n', 6) === 6, 'incrby');
    await r.mSet({ a: '1', b: '2' });
    check(eq(await r.mGet(['a', 'b', 'missing']), ['1', '2', null]), 'mget');
    check(await r.set('s', 'w', { NX: true }) === null, 'set nx');
  });
  await report(version, 'collections', async () => {
    await r.hSet('h', { f1: '1', f2: '2' });
    check(eq({ ...(await r.hGetAll('h')) }, { f1: '1', f2: '2' }), 'hgetall');
    await r.rPush('l', ['a', 'b', 'c']);
    check(eq(await r.lRange('l', 0, -1), ['a', 'b', 'c']), 'lrange');
    await r.sAdd('st', ['x', 'y']);
    check((await r.sMembers('st')).length === 2, 'smembers');
    await r.zAdd('z', { score: 1.5, value: 'm1' });
    check(await r.zScore('z', 'm1') === 1.5, 'zscore');
  });
  await report(version, 'pipeline', async () => {
    const m = r.multi();
    for (let i = 0; i < 1000; i++) m.set(`p:${i}`, String(i));
    await m.execAsPipeline();
    const g = r.multi();
    for (let i = 0; i < 1000; i++) g.get(`p:${i}`);
    const res = await g.execAsPipeline();
    check(res.every((v, i) => v === String(i)), 'pipelined gets');
  });
  await report(version, 'transaction', async () => {
    await r.set('tx', '0');
    const res = await r.multi().incr('tx').incr('tx').exec();
    check(res[1] === 2, 'multi/exec');
    const w = await fresh();
    await w.watch('tx');
    await r.set('tx', 'changed');
    let aborted = false;
    try { await w.multi().set('tx', 'mine').exec(); } catch (e) { aborted = /WatchError|aborted|watched keys/i.test(e.name + e.message); }
    check(aborted, 'watch conflict');
    await w.quit();
  });
  await report(version, 'pubsub', async () => {
    const sub = await fresh();
    const got = {};
    await sub.subscribe('ch', (m, ch) => { got[ch || 'ch'] = m; });
    await sub.pSubscribe('pat.*', (m, ch) => { got[ch] = m; });
    for (let i = 0; i < 25 && Object.keys(got).length < 2; i++) {
      await r.publish('ch', 'm1'); await r.publish('pat.x', 'm2'); await sleep(200);
    }
    await sub.quit();
    check(got.ch === 'm1' && got['pat.x'] === 'm2', JSON.stringify(got));
  });
  await report(version, 'scripting', async () => {
    check(await r.eval("return redis.call('SET', KEYS[1], ARGV[1])", { keys: ['lk'], arguments: ['lv'] }) === 'OK', 'eval');
    const sha = await r.scriptLoad("return redis.call('GET', KEYS[1])");
    check(await r.evalSha(sha, { keys: ['lk'] }) === 'lv', 'evalsha');
  });
  await report(version, 'blocking', async () => {
    await r.del('bq');
    const b = await fresh();
    setTimeout(() => r.rPush('bq', 'x'), 200);
    const v = await b.blPop('bq', 5);
    check(v && v.key === 'bq' && v.element === 'x', JSON.stringify(v));
    await b.quit();
  });
  await report(version, 'streams', async () => {
    await r.del('xs');
    await r.xAdd('xs', '*', { f: 'v' });
    await r.xGroupCreate('xs', 'g', '0');
    const got = await r.xReadGroup('g', 'c', { key: 'xs', id: '>' }, { COUNT: 10 });
    const stream = Array.isArray(got) ? got[0] : null;
    check(stream && stream.name === 'xs' && stream.messages.length === 1, JSON.stringify(got));
    check(await r.xAck('xs', 'g', stream.messages[0].id) === 1, 'xack');
    check((await r.xPending('xs', 'g')).pending === 0, 'xpending');
  });
  await report(version, 'scan', async () => {
    await r.flushAll();
    for (let i = 0; i < 500; i++) await r.set(`scan:${i}`, '1');
    const seen = new Set();
    for await (const ks of r.scanIterator({ MATCH: 'scan:*', COUNT: 50 })) {
      (Array.isArray(ks) ? ks : [ks]).forEach((k) => seen.add(k));
    }
    check(seen.size === 500, `scan saw ${seen.size}`);
  });
  await report(version, 'resp3', async () => {
    const r3 = await createClient({ url, RESP: 3 }).connect();
    await r3.hSet('r3h', 'f', '1');
    const h = await r3.hGetAll('r3h');
    check(h.f === '1' || (h instanceof Map && h.get('f') === '1'), JSON.stringify(h));
    await r3.zAdd('r3z', { score: 2.5, value: 'a' });
    check(await r3.zScore('r3z', 'a') === 2.5, 'zscore');
    check(await r3.get('missing') === null, 'null');
    await r3.quit();
  });
  await report(version, 'cluster', async () => {
    const rc = createCluster({ rootNodes: [{ url: `redis://127.0.0.1:${CPORT}` }] });
    await rc.connect();
    for (let i = 0; i < 200; i++) await rc.set(`ck:${i}`, String(i));
    for (let i = 0; i < 200; i++) check(await rc.get(`ck:${i}`) === String(i), `get ck:${i}`);
    await rc.mSet({ '{t}a': '1', '{t}b': '2' });
    check(eq(await rc.mGet(['{t}a', '{t}b']), ['1', '2']), 'mget');
    await rc.quit();
  });
  await r.quit();
}

(which === 'ioredis' ? ioredisSuite() : nodeRedisSuite())
  .catch((e) => { console.error(e); })
  .finally(() => setTimeout(() => process.exit(0), 100));
