/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Pepertect WebSocket Relay Server — Lightweight, Render Free Plan
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * Architecture:
 *   Upstox WS (binary) ──parse──► in-memory cache ──broadcast──► frontend WS clients
 *   Yahoo Finance (REST) ──fallback──► in-memory cache ──broadcast──► frontend WS clients
 *
 * Endpoints:
 *   GET  /health              — UptimeRobot keep-alive ping
 *   GET  /api/market/status   — Market status (REST fallback for frontend)
 *   GET  /api/options/chain   — Option chain proxy to Upstox
 *   WS   /ws?token=<jwt>      — Real-time market data WebSocket
 *
 * WS Protocol (Server → Client):
 *   { type: "auth:success", userId: "..." }
 *   { type: "market:update", data: { indices, stocks, timestamp, source } }
 *   { type: "market:derived", data: { gainers, losers, breadth, marketStatus, sectors, timestamp } }
 *   { type: "options:update", data: { ... } }
 *   { type: "error", message: "..." }
 *   { type: "ping" }
 *
 * WS Protocol (Client → Server):
 *   { type: "subscribe", channel: "market" | "options" | "positions", params?: {...} }
 *   { type: "unsubscribe", channel: "market" | "options" | "positions" }
 *   { type: "pong" }
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');

// ─── Configuration ──────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || '4000', 10);
const UPSTOX_ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN || '';
const UPSTOX_API_KEY = process.env.UPSTOX_API_KEY || '';
const UPSTOX_API_SECRET = process.env.UPSTOX_API_SECRET || '';
const UPSTOX_WS_URL = 'wss://api.upstox.com/v2/feed/market-data-feed';
const UPSTOX_REST_URL = 'https://api.upstox.com/v2';
const HEARTBEAT_SEC = 25;
const MARKET_POLL_INTERVAL_MS = 1500; // Yahoo Finance poll interval
const DERIVED_BROADCAST_INTERVAL_MS = 3000; // Derived data broadcast interval
const CLIENT_PING_INTERVAL_MS = 30000; // Ping clients every 30s
const CLIENT_PONG_TIMEOUT_MS = 10000; // Disconnect if no pong in 10s

// ─── Symbol Mappings ────────────────────────────────────────────────────────

const INDEX_NAMES = {
  'Nifty 50': 'NIFTY',
  'Nifty Bank': 'BANKNIFTY',
  'Nifty Fin Service': 'FINNIFTY',
  'Nifty IT': 'NIFTYIT',
  SENSEX: 'SENSEX',
  'Nifty Midcap 100': 'MIDCPNIFTY',
};

const YAHOO_INDEX_SYMBOLS = {
  NIFTY: '^NSEI', BANKNIFTY: '^NSEBANK', FINNIFTY: '^CNXFIN',
  SENSEX: '^BSESN', MIDCPNIFTY: '^NSMIDCAP',
};

const YAHOO_STOCK_SYMBOLS = {
  RELIANCE: 'RELIANCE.NS', TCS: 'TCS.NS', HDFCBANK: 'HDFCBANK.NS',
  INFY: 'INFY.NS', ICICIBANK: 'ICICIBANK.NS', HINDUNILVR: 'HINDUNILVR.NS',
  SBIN: 'SBIN.NS', BHARTIARTL: 'BHARTIARTL.NS', ITC: 'ITC.NS',
  KOTAKBANK: 'KOTAKBANK.NS', LT: 'LT.NS', AXISBANK: 'AXISBANK.NS',
  BAJFINANCE: 'BAJFINANCE.NS', ASIANPAINT: 'ASIANPAINT.NS', MARUTI: 'MARUTI.NS',
  SUNPHARMA: 'SUNPHARMA.NS', TATAMOTORS: 'TATAMOTORS.NS', WIPRO: 'WIPRO.NS',
  HCLTECH: 'HCLTECH.NS', ULTRACEMCO: 'ULTRACEMCO.NS', TITAN: 'TITAN.NS',
  NESTLEIND: 'NESTLEIND.NS', NTPC: 'NTPC.NS', POWERGRID: 'POWERGRID.NS',
  ONGC: 'ONGC.NS', TATASTEEL: 'TATASTEEL.NS', ADANIENT: 'ADANIENT.NS',
  ADANIPORTS: 'ADANIPORTS.NS', JSWSTEEL: 'JSWSTEEL.NS', COALINDIA: 'COALINDIA.NS',
  BPCL: 'BPCL.NS', HINDALCO: 'HINDALCO.NS', GRASIM: 'GRASIM.NS',
  TECHM: 'TECHM.NS', BAJAJFINSV: 'BAJAJFINSV.NS', DRREDDY: 'DRREDDY.NS',
  CIPLA: 'CIPLA.NS', EICHERMOT: 'EICHERMOT.NS', TATACONSUM: 'TATACONSUM.NS',
  HEROMOTOCO: 'HEROMOTOCO.NS', 'M&M': 'M&M.NS', APOLLOHOSP: 'APOLLOHOSP.NS',
  DIVISLAB: 'DIVISLAB.NS', BRITANNIA: 'BRITANNIA.NS', INDUSINDBK: 'INDUSINDBK.NS',
  HDFCLIFE: 'HDFCLIFE.NS', SBILIFE: 'SBILIFE.NS', YESBANK: 'YESBANK.NS',
  PNB: 'PNB.NS', BANKBARODA: 'BANKBARODA.NS', IDFCFIRSTB: 'IDFCFIRSTB.NS',
  SHRIRAMFIN: 'SHRIRAMFIN.NS', CHOLAFIN: 'CHOLAFIN.NS', SUZLON: 'SUZLON.NS',
  ADANIPOWER: 'ADANIPOWER.NS', HAL: 'HAL.NS', DMART: 'DMART.NS',
  TRENT: 'TRENT.NS', VEDL: 'VEDL.NS', SAIL: 'SAIL.NS',
  NMDC: 'NMDC.NS', IDEA: 'IDEA.NS', OIL: 'OIL.NS',
  GAIL: 'GAIL.NS', IOC: 'IOC.NS', PETRONET: 'PETRONET.NS',
};

// Reverse mapping: Yahoo symbol → our symbol
const YAHOO_TO_SYMBOL = {};
for (const [sym, yahoo] of Object.entries(YAHOO_STOCK_SYMBOLS)) {
  YAHOO_TO_SYMBOL[yahoo] = sym;
}
for (const [sym, yahoo] of Object.entries(YAHOO_INDEX_SYMBOLS)) {
  YAHOO_TO_SYMBOL[yahoo] = sym;
}

// Upstox instrument key mappings
const UPSTOX_INDICES = {
  NIFTY: 'NSE_INDEX|Nifty 50', BANKNIFTY: 'NSE_INDEX|Nifty Bank',
  FINNIFTY: 'NSE_INDEX|Nifty Fin Service', SENSEX: 'BSE_INDEX|SENSEX',
  MIDCPNIFTY: 'NSE_INDEX|Nifty Midcap 100',
};

const UPSTOX_STOCKS = {
  RELIANCE: 'NSE_EQ|INE002A01018', TCS: 'NSE_EQ|INE467B01029',
  HDFCBANK: 'NSE_EQ|INE040A01034', INFY: 'NSE_EQ|INE009A01021',
  ICICIBANK: 'NSE_EQ|INE090A01021', HINDUNILVR: 'NSE_EQ|INE030A01027',
  SBIN: 'NSE_EQ|INE062A01020', BHARTIARTL: 'NSE_EQ|INE738A01025',
  ITC: 'NSE_EQ|INE154A01025', KOTAKBANK: 'NSE_EQ|INE237A01028',
  LT: 'NSE_EQ|INE018A01030', AXISBANK: 'NSE_EQ|INE238A01034',
  BAJFINANCE: 'NSE_EQ|INE296A01024', ASIANPAINT: 'NSE_EQ|INE021A01026',
  MARUTI: 'NSE_EQ|INE585B01010', SUNPHARMA: 'NSE_EQ|INE044A01036',
  TATAMOTORS: 'NSE_EQ|INE155A01022', WIPRO: 'NSE_EQ|INE075A01022',
  HCLTECH: 'NSE_EQ|INE860A01027', ULTRACEMCO: 'NSE_EQ|INE406A01034',
  TITAN: 'NSE_EQ|INE280A01028', NESTLEIND: 'NSE_EQ|INE239A01042',
  NTPC: 'NSE_EQ|INE733A01031', POWERGRID: 'NSE_EQ|INE752E01010',
  ONGC: 'NSE_EQ|INE213A01029', TATASTEEL: 'NSE_EQ|INE081A01024',
  ADANIENT: 'NSE_EQ|INE423A01024', ADANIPORTS: 'NSE_EQ|INE742A01034',
  JSWSTEEL: 'NSE_EQ|INE019A01033', COALINDIA: 'NSE_EQ|INE522A01034',
  BPCL: 'NSE_EQ|INE029A01011', HINDALCO: 'NSE_EQ|INE038A01020',
  GRASIM: 'NSE_EQ|INE049A01031', TECHM: 'NSE_EQ|INE669C01020',
  BAJAJFINSV: 'NSE_EQ|INE298A01023', DRREDDY: 'NSE_EQ|INE088A01026',
  CIPLA: 'NSE_EQ|INE043A01027', EICHERMOT: 'NSE_EQ|INE066B01021',
  TATACONSUM: 'NSE_EQ|INE123A01022', HEROMOTOCO: 'INE158A01026',
  'M&M': 'NSE_EQ|INE101A01026', APOLLOHOSP: 'NSE_EQ|INE437B01018',
  DIVISLAB: 'NSE_EQ|INE363B01018', BRITANNIA: 'NSE_EQ|INE216A01030',
  INDUSINDBK: 'NSE_EQ|INE526A01015', HDFCLIFE: 'NSE_EQ|INE744G01013',
  SBILIFE: 'NSE_EQ|INE123B01016', YESBANK: 'NSE_EQ|INE528G01035',
  PNB: 'NSE_EQ|INE160A01015', BANKBARODA: 'NSE_EQ|INE028A01023',
  IDFCFIRSTB: 'NSE_EQ|INE092W01024', SHRIRAMFIN: 'NSE_EQ|INE745A01023',
  CHOLAFIN: 'NSE_EQ|INE324A01012', SUZLON: 'NSE_EQ|INE040D01025',
  ADANIPOWER: 'NSE_EQ|INE414E01016', HAL: 'NSE_EQ|INE095F01014',
  DMART: 'NSE_EQ|INE407L01015', TRENT: 'NSE_EQ|INE849A01017',
  VEDL: 'NSE_EQ|INE205A01024', SAIL: 'NSE_EQ|INE114A01011',
  NMDC: 'NSE_EQ|INE462B01014', IDEA: 'NSE_EQ|INE324A01026',
  OIL: 'NSE_EQ|INE274J01014', GAIL: 'INE129B01018',
  IOC: 'NSE_EQ|INE241A01010', PETRONET: 'INE267F01011',
};

// Build reverse map: instrument key → symbol
const INSTRUMENT_KEY_TO_SYMBOL = {};
for (const [sym, key] of Object.entries(UPSTOX_INDICES)) {
  INSTRUMENT_KEY_TO_SYMBOL[key] = sym;
  INSTRUMENT_KEY_TO_SYMBOL[key.replace('|', ':')] = sym;
}
for (const [sym, key] of Object.entries(UPSTOX_STOCKS)) {
  INSTRUMENT_KEY_TO_SYMBOL[key] = sym;
  INSTRUMENT_KEY_TO_SYMBOL[key.replace('|', ':')] = sym;
}

// ─── In-Memory Data Store ───────────────────────────────────────────────────

const state = {
  // Market data cache
  indices: {},
  stocks: {},
  activeSource: 'none', // 'upstox_ws' | 'yahoo' | 'none'

  // Connected clients
  clients: new Map(), // clientId → { ws, channels, lastPong }

  // Upstox WS connection
  upstoxWs: null,
  upstoxReconnectTimer: null,
  upstoxReconnectAttempt: 0,
  upstoxConnected: false,
  subscribedInstruments: new Set(),

  // Timers
  yahooPollTimer: null,
  derivedTimer: null,
  clientPingTimer: null,
  upstoxHeartbeatTimer: null,

  // Derived data cache
  cachedDerived: null,

  // Market poll state
  lastPollTime: 0,
  yahooErrors: 0,
  yahooAvailable: true,
  yahooIndexBatch: [],
  yahooStockBatches: [],

  // Option chain polling
  optionChainTimers: new Map(), // clientId → timer
};

let clientCounter = 0;

// ─── Utility Functions ──────────────────────────────────────────────────────

function getISTNow() {
  const now = new Date();
  const istOffset = 5.5 * 60 * 60 * 1000;
  return new Date(now.getTime() + istOffset + now.getTimezoneOffset() * 60000);
}

function computeMarketStatus() {
  const adjusted = getISTNow();
  const hours = adjusted.getHours();
  const minutes = adjusted.getMinutes();
  const day = adjusted.getDay();
  const timeInMinutes = hours * 60 + minutes;

  let status, message, nextOpen = null;

  if (day === 0 || day === 6) {
    status = 'CLOSED';
    message = day === 0 ? 'Market closed - Sunday' : 'Market closed - Saturday';
    const daysUntilMonday = day === 0 ? 1 : 2;
    const nextMonday = new Date(adjusted);
    nextMonday.setDate(adjusted.getDate() + daysUntilMonday);
    nextOpen = `${nextMonday.toISOString().split('T')[0]}T09:15:00+05:30`;
  } else if (timeInMinutes >= 540 && timeInMinutes < 555) {
    status = 'PRE-OPEN';
    message = 'Pre-open session (9:00 - 9:15 IST)';
  } else if (timeInMinutes >= 555 && timeInMinutes < 930) {
    status = 'OPEN';
    message = 'Market is open (9:15 - 15:30 IST)';
  } else if (timeInMinutes >= 930 && timeInMinutes < 960) {
    status = 'POST-CLOSE';
    message = 'Post-close session (15:30 - 16:00 IST)';
  } else if (timeInMinutes < 540) {
    status = 'CLOSED';
    message = 'Market opens at 9:00 IST (Pre-open session)';
  } else {
    status = 'CLOSED';
    message = 'Market closed for the day';
  }

  return { status, message, istTime: adjusted.toISOString(), nextOpen };
}

function isMarketOpen() {
  const ms = computeMarketStatus();
  return ms.status === 'OPEN' || ms.status === 'PRE-OPEN';
}

function computeDerivedData() {
  const stockEntries = Object.entries(state.stocks);
  if (stockEntries.length === 0) return null;

  const enriched = [];
  for (const [symbol, data] of stockEntries) {
    const lastPrice = data.last_price || 0;
    const prevClose = data.ohlc?.close || 0;
    const change = data.net_change || (lastPrice - prevClose);
    const changePercent = prevClose !== 0 ? (change / prevClose) * 100 : 0;
    enriched.push({
      symbol,
      name: symbol, // No DB for names in this lightweight server
      currentPrice: lastPrice,
      change: Math.round(change * 100) / 100,
      changePercent: Math.round(changePercent * 100) / 100,
      volume: data.volume || null,
    });
  }

  enriched.sort((a, b) => b.changePercent - a.changePercent);
  const gainers = enriched.slice(0, 5);
  const losers = enriched.length > 5
    ? enriched.slice(-5).reverse()
    : enriched.filter(e => e.changePercent < 0).slice(0, 5);

  let advances = 0, declines = 0, unchanged = 0;
  for (const e of enriched) {
    if (e.changePercent > 0) advances++;
    else if (e.changePercent < 0) declines++;
    else unchanged++;
  }

  const marketStatus = computeMarketStatus();

  return {
    gainers,
    losers,
    breadth: { advances, declines, unchanged },
    marketStatus,
    sectors: [], // No DB for sectors in this lightweight server
    timestamp: Date.now(),
  };
}

function decodeJWT(token) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const payload = Buffer.from(parts[1], 'base64url').toString('utf-8');
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

// ─── Broadcast Helper ───────────────────────────────────────────────────────

function broadcastToChannel(channel, message) {
  const msgStr = typeof message === 'string' ? message : JSON.stringify(message);
  for (const [id, client] of state.clients) {
    if (client.ws.readyState === WebSocket.OPEN && client.channels.has(channel)) {
      try { client.ws.send(msgStr); } catch {}
    }
  }
}

function sendToClient(clientId, message) {
  const client = state.clients.get(clientId);
  if (client && client.ws.readyState === WebSocket.OPEN) {
    try { client.ws.send(typeof message === 'string' ? message : JSON.stringify(message)); } catch {}
  }
}

// ─── Yahoo Finance Data Fetching ────────────────────────────────────────────

function prepareYahooBatches() {
  state.yahooIndexBatch = Object.entries(YAHOO_INDEX_SYMBOLS).map(
    ([sym, yahoo]) => `${sym}::${yahoo}`
  );
  const stockEntries = Object.entries(YAHOO_STOCK_SYMBOLS).map(
    ([sym, yahoo]) => `${sym}::${yahoo}`
  );
  state.yahooStockBatches = [];
  for (let i = 0; i < stockEntries.length; i += 20) {
    state.yahooStockBatches.push(stockEntries.slice(i, i + 20));
  }
}

async function fetchYahooBatch(batch, results, isIndex) {
  const yahooSymbols = batch.map(e => encodeURIComponent(e.split('::')[1]));
  const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${yahooSymbols.join(',')}`;

  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(4000),
    });

    if (!res.ok) {
      await fetchYahooChartBatch(batch, results, isIndex);
      return;
    }

    const json = await res.json();
    for (const quote of (json?.quoteResponse?.result || [])) {
      const symbol = YAHOO_TO_SYMBOL[quote.symbol];
      if (!symbol) continue;
      results[symbol] = {
        last_price: quote.regularMarketPrice || 0,
        net_change: quote.regularMarketChange || 0,
        ohlc: {
          open: quote.regularMarketOpen || quote.regularMarketPreviousClose || 0,
          high: quote.regularMarketDayHigh || quote.regularMarketPrice || 0,
          low: quote.regularMarketDayLow || quote.regularMarketPrice || 0,
          close: quote.regularMarketPreviousClose || 0,
        },
        volume: quote.regularMarketVolume || null,
        ...(isIndex ? {} : { oi: null }),
      };
    }
  } catch {
    await fetchYahooChartBatch(batch, results, isIndex);
  }
}

async function fetchYahooChartBatch(batch, results, isIndex) {
  const promises = batch.map(async (entry) => {
    const [symbol, yahooSymbol] = entry.split('::');
    try {
      const chartUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSymbol)}?interval=1m&range=1d`;
      const res = await fetch(chartUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json',
        },
        signal: AbortSignal.timeout(2500),
      });
      if (!res.ok) return;
      const json = await res.json();
      const meta = json?.chart?.result?.[0]?.meta;
      if (!meta) return;
      const price = meta.regularMarketPrice || 0;
      const prevClose = meta.previousClose || meta.chartPreviousClose || price;
      results[symbol] = {
        last_price: price,
        net_change: price - prevClose,
        ohlc: {
          open: meta.regularMarketOpen || prevClose,
          high: meta.regularMarketDayHigh || price,
          low: meta.regularMarketDayLow || price,
          close: prevClose,
        },
        volume: meta.regularMarketVolume || null,
        ...(isIndex ? {} : { oi: null }),
      };
    } catch {}
  });
  await Promise.allSettled(promises);
}

async function pollYahooFinance() {
  const now = Date.now();
  if (now - state.lastPollTime < MARKET_POLL_INTERVAL_MS * 0.8) return;
  state.lastPollTime = now;

  try {
    const allResults = {};
    const batchPromises = [
      fetchYahooBatch(state.yahooIndexBatch, allResults, true),
      ...state.yahooStockBatches.map(batch => fetchYahooBatch(batch, allResults, false)),
    ];
    await Promise.allSettled(batchPromises);

    const newIndices = {};
    const newStocks = {};
    const indexSymbols = new Set(Object.keys(YAHOO_INDEX_SYMBOLS));

    for (const [symbol, data] of Object.entries(allResults)) {
      if (indexSymbols.has(symbol)) newIndices[symbol] = data;
      else newStocks[symbol] = data;
    }

    if (Object.keys(newIndices).length > 0 || Object.keys(newStocks).length > 0) {
      Object.assign(state.indices, newIndices);
      Object.assign(state.stocks, newStocks);

      // If Upstox WS is not connected, use Yahoo as primary source
      if (!state.upstoxConnected) {
        state.activeSource = 'yahoo';
        broadcastMarketUpdate();
      }
    }

    state.yahooErrors = 0;
    state.yahooAvailable = true;
  } catch {
    state.yahooErrors++;
    if (state.yahooErrors >= 5) {
      state.yahooAvailable = false;
      console.warn('[Yahoo] Unavailable after 5 consecutive errors');
    }
  }
}

// ─── Market Update Broadcast ────────────────────────────────────────────────

function broadcastMarketUpdate() {
  if (state.clients.size === 0) return;

  const hasMarketSubscribers = [...state.clients.values()].some(c => c.channels.has('market'));
  if (!hasMarketSubscribers) return;

  const data = {
    indices: { ...state.indices },
    stocks: { ...state.stocks },
    timestamp: Date.now(),
    source: state.activeSource,
  };

  broadcastToChannel('market', { type: 'market:update', data });
}

function broadcastDerivedData() {
  if (state.clients.size === 0) return;

  const hasMarketSubscribers = [...state.clients.values()].some(c => c.channels.has('market'));
  if (!hasMarketSubscribers) return;

  const derived = computeDerivedData();
  if (!derived) return;

  state.cachedDerived = derived;
  broadcastToChannel('market', { type: 'market:derived', data: derived });
}

// ─── Upstox WebSocket (Binary Protocol) ─────────────────────────────────────

function connectUpstoxWS() {
  if (!UPSTOX_ACCESS_TOKEN) {
    console.log('[UpstoxWS] No access token configured — using Yahoo Finance only');
    return;
  }

  try {
    console.log('[UpstoxWS] Connecting to Upstox feed...');
    const ws = new WebSocket(UPSTOX_WS_URL);

    ws.on('open', () => {
      console.log('[UpstoxWS] Connected');
      state.upstoxWs = ws;
      state.upstoxConnected = true;
      state.upstoxReconnectAttempt = 0;

      // Auth
      ws.send(JSON.stringify({ Authorization: `Bearer ${UPSTOX_ACCESS_TOKEN}` }));

      // Subscribe to default instruments
      subscribeUpstoxInstruments(getDefaultUpstoxInstruments());

      // Start heartbeat
      startUpstoxHeartbeat();
    });

    ws.on('message', (raw) => {
      try {
        if (typeof raw === 'string') {
          // String message — subscribe/unsubscribe response
          const msg = JSON.parse(raw);
          if (msg.status === 'error') {
            console.error('[UpstoxWS] Error:', msg);
          }
          return;
        }

        // Binary message — market data
        handleUpstoxBinaryMessage(raw);
      } catch (err) {
        console.error('[UpstoxWS] Message parse error:', err);
      }
    });

    ws.on('close', (event) => {
      console.log(`[UpstoxWS] Closed: code=${event.code} reason="${event.reason}"`);
      state.upstoxWs = null;
      state.upstoxConnected = false;
      stopUpstoxHeartbeat();
      scheduleUpstoxReconnect();
    });

    ws.on('error', (err) => {
      console.error('[UpstoxWS] Error:', err.message);
      // close event will follow
    });
  } catch (err) {
    console.error('[UpstoxWS] Connect failed:', err);
    scheduleUpstoxReconnect();
  }
}

function getDefaultUpstoxInstruments() {
  return [
    ...Object.values(UPSTOX_INDICES),
    ...Object.values(UPSTOX_STOCKS),
  ];
}

function subscribeUpstoxInstruments(keys) {
  if (!state.upstoxWs || state.upstoxWs.readyState !== WebSocket.OPEN) return;
  const newKeys = keys.filter(k => !state.subscribedInstruments.has(k));
  if (newKeys.length === 0) return;

  const msg = {
    guid: crypto.randomUUID(),
    method: 'sub',
    data: { mode: 'full', instrumentKeys: newKeys },
  };
  state.upstoxWs.send(JSON.stringify(msg));
  newKeys.forEach(k => state.subscribedInstruments.add(k));
  console.log(`[UpstoxWS] Subscribed to ${newKeys.length} instruments (total: ${state.subscribedInstruments.size})`);
}

function handleUpstoxBinaryMessage(data) {
  const view = new DataView(data.buffer || data);

  if (data.byteLength < 5) {
    console.error('[UpstoxWS] Binary packet too short:', data.byteLength);
    return;
  }

  // Byte 2: Feed type (1=full, 2=ltp, 3=quote)
  const feedTypeByte = view.getUint8(2);

  // Byte 3-4: Instrument key length (big-endian)
  const keyLength = view.getUint16(3, false);

  // Byte 5 to 5+keyLength-1: Instrument key (UTF-8)
  const keyBytes = new Uint8Array(data.buffer || data, 5, keyLength);
  const instrumentKey = new TextDecoder().decode(keyBytes);

  // Remaining bytes: JSON payload
  const jsonStart = 5 + keyLength;
  const jsonBytes = new Uint8Array(data.buffer || data, jsonStart);
  const jsonString = new TextDecoder().decode(jsonBytes);

  let parsed;
  try {
    parsed = JSON.parse(jsonString);
  } catch {
    return;
  }

  // Map instrument key to our symbol
  let symbol = INSTRUMENT_KEY_TO_SYMBOL[instrumentKey];
  if (!symbol) {
    // Try extracting from the key name
    const parts = instrumentKey.split('|');
    const name = parts[1] || parts[0];
    symbol = INDEX_NAMES[name] || name.toUpperCase().replace(/\s+/g, '');
  }

  // Extract data
  const ff = parsed.ff || {};
  const ltp = parsed.ltp || 0;
  const bid = ff.bid_price || parsed.bp || 0;
  const ask = ff.ask_price || parsed.ap || 0;

  const quoteData = {
    last_price: ltp,
    net_change: ff.net_change || 0,
    ohlc: {
      open: parsed.open || ff.open_price || 0,
      high: parsed.high || ff.high_price || 0,
      low: parsed.low || ff.low_price || 0,
      close: parsed.close || ff.close_price || 0,
    },
    volume: parsed.volume || ff.total_traded_volume || null,
    ...(ff.open_interest !== undefined ? { oi: ff.open_interest } : { oi: null }),
  };

  // Update cache
  const indexSymbols = new Set(Object.keys(YAHOO_INDEX_SYMBOLS));
  if (indexSymbols.has(symbol)) {
    state.indices[symbol] = quoteData;
  } else {
    state.stocks[symbol] = quoteData;
  }

  // Mark as upstox source
  state.activeSource = 'upstox_ws';

  // Broadcast immediately
  broadcastMarketUpdate();
}

function scheduleUpstoxReconnect() {
  if (state.upstoxReconnectTimer) return;

  state.upstoxReconnectAttempt++;
  if (state.upstoxReconnectAttempt > 20) {
    console.error('[UpstoxWS] Max reconnect attempts reached. Using Yahoo Finance only.');
    return;
  }

  const delay = Math.min(1000 * Math.pow(2, state.upstoxReconnectAttempt - 1), 60000);
  const jitter = Math.random() * delay * 0.3;
  const totalDelay = delay + jitter;

  console.log(`[UpstoxWS] Reconnecting in ${Math.round(totalDelay)}ms (attempt ${state.upstoxReconnectAttempt}/20)`);

  state.upstoxReconnectTimer = setTimeout(() => {
    state.upstoxReconnectTimer = null;
    connectUpstoxWS();
  }, totalDelay);
}

function startUpstoxHeartbeat() {
  stopUpstoxHeartbeat();
  state.upstoxHeartbeatTimer = setInterval(() => {
    if (state.upstoxWs && state.upstoxWs.readyState === WebSocket.OPEN) {
      state.upstoxWs.send('{"ping":true}');
    }
  }, HEARTBEAT_SEC * 1000);
}

function stopUpstoxHeartbeat() {
  if (state.upstoxHeartbeatTimer) {
    clearInterval(state.upstoxHeartbeatTimer);
    state.upstoxHeartbeatTimer = null;
  }
}

// ─── Option Chain Proxy (Upstox REST) ──────────────────────────────────────

async function fetchOptionChain(underlying, expiry) {
  if (!UPSTOX_ACCESS_TOKEN) return null;

  try {
    let url = `${UPSTOX_REST_URL}/option/chain?instrument_key=NSE_INDEX|${underlying}`;
    if (expiry) url += `&expiry_date=${encodeURIComponent(expiry)}`;

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${UPSTOX_ACCESS_TOKEN}`,
        Accept: 'application/json',
      },
      signal: AbortSignal.timeout(10000),
    });

    if (!res.ok) return null;
    const json = await res.json();
    return json.data || null;
  } catch (err) {
    console.error('[OptionChain] Fetch error:', err);
    return null;
  }
}

async function pollOptionChainForClient(clientId, underlying, expiry) {
  const data = await fetchOptionChain(underlying, expiry);
  if (data) {
    // Extract relevant data from Upstox response and send to client
    const spotPrice = data.underlying_value || 0;
    const strikes = (data.option_chain_data || []).map(s => ({
      strike_price: s.strike_price,
      call_options: {
        market_data: {
          ltp: s.call_options?.market_data?.ltp || 0,
          close_price: s.call_options?.market_data?.close || 0,
          volume: s.call_options?.market_data?.volume_traded || 0,
          oi: s.call_options?.market_data?.open_interest || 0,
          prev_oi: s.call_options?.market_data?.previous_day_open_interest || 0,
          bid_price: s.call_options?.market_data?.bid_price || 0,
          ask_price: s.call_options?.market_data?.ask_price || 0,
        },
        option_greeks: s.call_options?.option_greeks || {},
      },
      put_options: {
        market_data: {
          ltp: s.put_options?.market_data?.ltp || 0,
          close_price: s.put_options?.market_data?.close || 0,
          volume: s.put_options?.market_data?.volume_traded || 0,
          oi: s.put_options?.market_data?.open_interest || 0,
          prev_oi: s.put_options?.market_data?.previous_day_open_interest || 0,
          bid_price: s.put_options?.market_data?.bid_price || 0,
          ask_price: s.put_options?.market_data?.ask_price || 0,
        },
        option_greeks: s.put_options?.option_greeks || {},
      },
    }));

    const update = {
      underlying,
      expiry: data.expiry_date || expiry || '',
      spot: spotPrice,
      pcr: data.pcr || 0,
      maxPainStrike: 0, // Compute if available
      strikes,
      timestamp: Date.now(),
    };

    sendToClient(clientId, { type: 'options:update', data: update });
  }
}

// ─── Positions Proxy (Upstox REST) ─────────────────────────────────────────

async function fetchPositions() {
  if (!UPSTOX_ACCESS_TOKEN) return [];

  try {
    const res = await fetch(`${UPSTOX_REST_URL}/portfolio/positions`, {
      headers: {
        Authorization: `Bearer ${UPSTOX_ACCESS_TOKEN}`,
        Accept: 'application/json',
      },
      signal: AbortSignal.timeout(10000),
    });

    if (!res.ok) return [];
    const json = await res.json();
    return json.data || [];
  } catch {
    return [];
  }
}

// ─── HTTP Server ────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const url = new URL(req.url, `http://${req.headers.host}`);

  // Health check — for UptimeRobot
  if (url.pathname === '/health') {
    const upstoxStatus = state.upstoxConnected ? 'connected' : 'disconnected';
    const clientCount = state.clients.size;
    const yahooStatus = state.yahooAvailable ? 'available' : 'unavailable';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptime: process.uptime(),
      clients: clientCount,
      upstoxWs: upstoxStatus,
      yahooFinance: yahooStatus,
      activeSource: state.activeSource,
      memoryMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      timestamp: Date.now(),
    }));
    return;
  }

  // Market status endpoint — REST fallback for frontend
  if (url.pathname === '/api/market/status') {
    const marketStatus = computeMarketStatus();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ data: marketStatus }));
    return;
  }

  // Option chain REST endpoint
  if (url.pathname === '/api/options/chain') {
    const underlying = url.searchParams.get('underlying') || 'Nifty 50';
    const expiry = url.searchParams.get('expiry') || '';
    const data = await fetchOptionChain(underlying, expiry);
    if (data) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data }));
    } else {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Upstox API unavailable' }));
    }
    return;
  }

  // Positions REST endpoint
  if (url.pathname === '/api/positions') {
    const positions = await fetchPositions();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ data: positions }));
    return;
  }

  // 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

// ─── WebSocket Server ───────────────────────────────────────────────────────

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const clientId = `client_${++clientCounter}`;
  console.log(`[WS] Client ${clientId} connected from ${req.socket.remoteAddress}`);

  // Parse token from query params
  const url = new URL(req.url, `ws://${req.headers.host}`);
  const token = url.searchParams.get('token') || '';

  // Decode JWT to get userId (no verification for MVP — auth is handled by Next.js app)
  let userId = 'anonymous';
  if (token) {
    const decoded = decodeJWT(token);
    if (decoded?.userId || decoded?.sub || decoded?.id) {
      userId = decoded.userId || decoded.sub || decoded.id;
    } else if (decoded?.email) {
      userId = decoded.email;
    }
  }

  // Register client
  const client = {
    ws,
    channels: new Set(),
    lastPong: Date.now(),
    userId,
    params: {},
  };
  state.clients.set(clientId, client);

  // Send auth success
  ws.send(JSON.stringify({ type: 'auth:success', userId }));

  // Send initial cached data if available
  if (Object.keys(state.indices).length > 0 || Object.keys(state.stocks).length > 0) {
    ws.send(JSON.stringify({
      type: 'market:initial',
      data: {
        indices: state.indices,
        stocks: state.stocks,
        timestamp: Date.now(),
        source: state.activeSource,
      },
    }));
  }

  // Send cached derived data
  if (state.cachedDerived) {
    ws.send(JSON.stringify({ type: 'market:derived', data: state.cachedDerived }));
  }

  // Handle messages from client
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      switch (msg.type) {
        case 'subscribe': {
          const channel = msg.channel;
          const params = msg.params || {};
          client.channels.add(channel);
          client.params = { ...client.params, ...params };

          console.log(`[WS] ${clientId} subscribed to ${channel}`, params.underlying ? `(${params.underlying})` : '');

          // If market channel, send initial data and trigger immediate poll
          if (channel === 'market') {
            // Send latest data immediately
            if (Object.keys(state.indices).length > 0 || Object.keys(state.stocks).length > 0) {
              ws.send(JSON.stringify({
                type: 'market:update',
                data: {
                  indices: state.indices,
                  stocks: state.stocks,
                  timestamp: Date.now(),
                  source: state.activeSource,
                },
              }));
            }
            if (state.cachedDerived) {
              ws.send(JSON.stringify({ type: 'market:derived', data: state.cachedDerived }));
            }
          }

          // If options channel, start polling option chain
          if (channel === 'options') {
            const underlying = params.underlying || 'Nifty 50';
            const expiry = params.expiry || '';
            // Immediate fetch
            pollOptionChainForClient(clientId, underlying, expiry);
            // Then poll every 5 seconds
            const timer = setInterval(() => {
              if (state.clients.has(clientId) && client.channels.has('options')) {
                pollOptionChainForClient(clientId, underlying, expiry);
              } else {
                clearInterval(timer);
              }
            }, 5000);
            state.optionChainTimers.set(clientId, timer);
          }

          // If positions channel, start polling
          if (channel === 'positions') {
            const pollPositions = async () => {
              if (!state.clients.has(clientId) || !client.channels.has('positions')) return;
              const positions = await fetchPositions();
              sendToClient(clientId, { type: 'positions', data: positions });
            };
            pollPositions();
            const timer = setInterval(pollPositions, 10000);
            state.optionChainTimers.set(`positions_${clientId}`, timer);
          }

          // Send subscribed confirmation
          ws.send(JSON.stringify({ type: 'subscribed', channel }));
          break;
        }

        case 'unsubscribe': {
          const channel = msg.channel;
          client.channels.delete(channel);
          console.log(`[WS] ${clientId} unsubscribed from ${channel}`);

          // Stop option chain polling if applicable
          if (channel === 'options') {
            const timer = state.optionChainTimers.get(clientId);
            if (timer) { clearInterval(timer); state.optionChainTimers.delete(clientId); }
          }
          if (channel === 'positions') {
            const timer = state.optionChainTimers.get(`positions_${clientId}`);
            if (timer) { clearInterval(timer); state.optionChainTimers.delete(`positions_${clientId}`); }
          }

          ws.send(JSON.stringify({ type: 'unsubscribed', channel }));
          break;
        }

        case 'pong':
          client.lastPong = Date.now();
          break;
      }
    } catch {
      // Ignore malformed messages
    }
  });

  ws.on('close', (code, reason) => {
    console.log(`[WS] Client ${clientId} disconnected: code=${code}`);
    state.clients.delete(clientId);

    // Clean up timers
    const timer = state.optionChainTimers.get(clientId);
    if (timer) { clearInterval(timer); state.optionChainTimers.delete(clientId); }
    const posTimer = state.optionChainTimers.get(`positions_${clientId}`);
    if (posTimer) { clearInterval(posTimer); state.optionChainTimers.delete(`positions_${clientId}`); }
  });

  ws.on('error', (err) => {
    console.error(`[WS] Client ${clientId} error:`, err.message);
  });
});

// ─── Client Ping/Pong ───────────────────────────────────────────────────────

function startClientPing() {
  state.clientPingTimer = setInterval(() => {
    const now = Date.now();
    for (const [id, client] of state.clients) {
      if (client.ws.readyState !== WebSocket.OPEN) continue;

      // Check pong timeout
      if (now - client.lastPong > CLIENT_PONG_TIMEOUT_MS + CLIENT_PING_INTERVAL_MS) {
        console.log(`[WS] Client ${id} pong timeout — disconnecting`);
        client.ws.terminate();
        state.clients.delete(id);
        continue;
      }

      // Send ping
      try { client.ws.send(JSON.stringify({ type: 'ping' })); } catch {}
    }
  }, CLIENT_PING_INTERVAL_MS);
}

// ─── Startup ────────────────────────────────────────────────────────────────

function start() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Pepertect WebSocket Relay Server');
  console.log('═══════════════════════════════════════════════════');
  console.log(`  PORT: ${PORT}`);
  console.log(`  Upstox Token: ${UPSTOX_ACCESS_TOKEN ? 'configured' : 'NOT configured'}`);
  console.log(`  Upstox API Key: ${UPSTOX_API_KEY ? 'configured' : 'NOT configured'}`);
  console.log(`  Node.js: ${process.version}`);
  console.log('═══════════════════════════════════════════════════');

  // Prepare Yahoo batches
  prepareYahooBatches();

  // Start Yahoo Finance polling (always running as fallback)
  state.yahooPollTimer = setInterval(() => pollYahooFinance(), MARKET_POLL_INTERVAL_MS);
  pollYahooFinance(); // Immediate first poll
  console.log(`[Yahoo] Started polling every ${MARKET_POLL_INTERVAL_MS}ms`);

  // Start derived data broadcast
  state.derivedTimer = setInterval(() => broadcastDerivedData(), DERIVED_BROADCAST_INTERVAL_MS);
  console.log(`[Derived] Broadcasting every ${DERIVED_BROADCAST_INTERVAL_MS}ms`);

  // Start client ping
  startClientPing();

  // Connect to Upstox WS (if token available)
  if (UPSTOX_ACCESS_TOKEN) {
    connectUpstoxWS();
  }

  // Start HTTP server
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n[Server] Listening on http://0.0.0.0:${PORT}`);
    console.log(`[Server] WebSocket endpoint: ws://0.0.0.0:${PORT}/ws`);
    console.log(`[Server] Health check: http://0.0.0.0:${PORT}/health\n`);
  });
}

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('\n[Server] SIGTERM received — shutting down...');
  if (state.yahooPollTimer) clearInterval(state.yahooPollTimer);
  if (state.derivedTimer) clearInterval(state.derivedTimer);
  if (state.clientPingTimer) clearInterval(state.clientPingTimer);
  stopUpstoxHeartbeat();
  if (state.upstoxWs) state.upstoxWs.close();
  for (const timer of state.optionChainTimers.values()) clearInterval(timer);
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000);
});

process.on('SIGINT', () => {
  console.log('\n[Server] SIGINT received — shutting down...');
  if (state.yahooPollTimer) clearInterval(state.yahooPollTimer);
  if (state.derivedTimer) clearInterval(state.derivedTimer);
  if (state.clientPingTimer) clearInterval(state.clientPingTimer);
  stopUpstoxHeartbeat();
  if (state.upstoxWs) state.upstoxWs.close();
  for (const timer of state.optionChainTimers.values()) clearInterval(timer);
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000);
});

// Start the server
start();