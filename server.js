
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const ccxt = require('ccxt');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));

const MODE = process.env.BINANCE_MODE || 'testnet';

// V4.1 ULTRA + TRAIL ACT %
let BOT_CONFIG = {
  leverage: parseInt(process.env.DEFAULT_LEVERAGE) || 10,
  marginUsdt: parseFloat(process.env.DEFAULT_MARGIN_USDT) || 20,
  tpPercent: parseFloat(process.env.DEFAULT_TP_PERCENT) || 0.18,
  slPercent: parseFloat(process.env.DEFAULT_SL_PERCENT) || 0.6,
  maxPositions: parseInt(process.env.MAX_OPEN_POSITIONS) || 3,
  minUsdt: parseFloat(process.env.MIN_USDT_PER_TRADE) || 1,
  isRunning: false,
  confidenceThreshold: 45,
  ultraMode: true,
  turboMode: true,
  scanIntervalMs: 2500,
  // NEW TRAILING
  trailActPercent: parseFloat(process.env.TRAIL_ACT_PERCENT) || 0.12,
  trailDistancePercent: parseFloat(process.env.TRAIL_DISTANCE_PERCENT) || 0.08,
  trailEnabled: true
};

const TOP_PAIRS = [
  'BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT','WIF/USDT','BONK/USDT',
  'DOGE/USDT','SHIB/USDT','FLOKI/USDT','1000PEPE/USDT','1000SHIB/USDT',
  'AVAX/USDT','LINK/USDT','ARB/USDT','OP/USDT','SUI/USDT','TIA/USDT','SEI/USDT','APT/USDT','NEAR/USDT'
];

let exchange = null;
let balance = { total: 0, free: 0, pnl: 0, status: 'disconnected' };
let positions = [];
let tradeHistory = [];
let marketData = {};
let scanLogs = [];
let lastTradeTime = 0;
let priceCache = {};
let totalProfit = 0;
let winCount = 0, lossCount = 0;
let trailingMap = {}; // symbol -> { active, maxPrice, minPrice, stopPrice, actPrice }

function log(msg, type='info') {
  const entry = { time: new Date().toLocaleTimeString(), msg, type };
  scanLogs.unshift(entry);
  if(scanLogs.length>90) scanLogs.pop();
  io.emit('log', entry);
  console.log(entry.time, msg);
}

function initExchange(apiKey, apiSecret, mode) {
  const isTest = mode === 'testnet';
  exchange = new ccxt.binance({
    apiKey, secret: apiSecret,
    enableRateLimit: true,
    options: { defaultType: 'future', adjustForTimeDifference: true },
  });
  if(isTest) exchange.setSandboxMode(true);
  return exchange;
}

async function fetchBalance() {
  if(!exchange) { io.emit('balance', balance); return; }
  try {
    const bal = await exchange.fetchBalance({ type: 'future' });
    balance.total = bal?.USDT?.total || 0;
    balance.free = bal?.USDT?.free || 0;
    balance.status = 'connected';
    const poss = await exchange.fetchPositions();
    let totalPnl = 0;
    poss.forEach(p => { if(p.unrealizedPnl) totalPnl += p.unrealizedPnl; });
    balance.pnl = totalPnl;
    positions = poss.filter(p => Math.abs(parseFloat(p.contracts || 0)) > 0);
    io.emit('balance', { ...balance, totalProfit, winCount, lossCount, totalTrades: tradeHistory.length });
    io.emit('positions', positions.map(p=> ({...p, trailing: trailingMap[p.symbol] || null})) );
  } catch(e) { 
    balance.status = 'error: '+e.message.slice(0,80);
    log('Balance error: '+e.message, 'error');
    io.emit('balance', balance);
  }
}

function calculateUltraSignal(candles, pair) {
  if(candles.length < 20) return { signal: 'HOLD', confidence: 0, reason: 'wait' };
  const closes = candles.map(c => c[4]);
  const last = closes.length-1;
  const price = closes[last];
  const prev1 = closes[last-1];
  const prev2 = closes[last-2];
  const mom1 = ((price - prev1)/prev1)*100;
  const mom2 = ((price - prev2)/prev2)*100;
  const ema = (arr, period) => {
    const k = 2/(period+1);
    let e=[arr[0]];
    for(let i=1;i<arr.length;i++) e.push(arr[i]*k + e[i-1]*(1-k));
    return e;
  };
  const ema9 = ema(closes,9);
  const ema21 = ema(closes,21);
  const ema9Now = ema9[last], ema21Now = ema21[last];
  const ema9Prev = ema9[last-1], ema21Prev = ema21[last-1];
  const rsi = (() => {
    let gains=0, losses=0;
    for(let i=1;i<=14 && i<closes.length;i++){
      const diff = closes[i]-closes[i-1];
      if(diff>=0) gains+=diff; else losses-=diff;
    }
    let avgG = gains/14, avgL = losses/14 || 0.001;
    for(let i=15;i<closes.length;i++){
      const diff = closes[i]-closes[i-1];
      if(diff>=0){ avgG = (avgG*13+diff)/14; avgL = (avgL*13)/14; }
      else { avgG = (avgG*13)/14; avgL = (avgL*13-diff)/14; }
    }
    const rs = avgG/(avgL||0.001);
    return 100 - (100/(1+rs));
  })();
  const cached = priceCache[pair];
  let instantMom = 0;
  if(cached) instantMom = ((price - cached.price)/cached.price)*100;
  priceCache[pair] = { price, time: Date.now() };
  let signal='HOLD', confidence=0, reason='';
  if(mom1 > 0.12 && mom2 > 0.05 && ema9Now > ema21Now && rsi < 75) {
    signal='LONG'; confidence= Math.min(85, 60 + Math.abs(mom1)*100); reason=`MOM +${mom1.toFixed(3)}% EMA up RSI ${rsi.toFixed(0)}`;
  } else if(mom1 < -0.12 && mom2 < -0.05 && ema9Now < ema21Now && rsi > 25) {
    signal='SHORT'; confidence= Math.min(85, 60 + Math.abs(mom1)*100); reason=`MOM ${mom1.toFixed(3)}% EMA down RSI ${rsi.toFixed(0)}`;
  } else if(instantMom > 0.18 && rsi < 72) {
    signal='LONG'; confidence=72; reason=`PUMP +${instantMom.toFixed(3)}%`;
  } else if(instantMom < -0.18 && rsi > 28) {
    signal='SHORT'; confidence=72; reason=`DUMP ${instantMom.toFixed(3)}%`;
  } else if(ema9Prev <= ema21Prev && ema9Now > ema21Now && rsi > 30 && rsi < 70) {
    signal='LONG'; confidence=62; reason=`EMA CROSS UP RSI ${rsi.toFixed(0)}`;
  } else if(ema9Prev >= ema21Prev && ema9Now < ema21Now && rsi < 70 && rsi > 30) {
    signal='SHORT'; confidence=62; reason=`EMA CROSS DOWN RSI ${rsi.toFixed(0)}`;
  } else if(price > ema9Now && ema9Now > ema21Now && mom1 > 0.04 && rsi > 45 && rsi < 68) {
    signal='LONG'; confidence=55; reason=`TREND LONG ${mom1.toFixed(3)}%`;
  } else if(price < ema9Now && ema9Now < ema21Now && mom1 < -0.04 && rsi < 55 && rsi > 32) {
    signal='SHORT'; confidence=55; reason=`TREND SHORT ${mom1.toFixed(3)}%`;
  } else {
    reason=`WAIT ${mom1.toFixed(3)}% RSI ${rsi.toFixed(0)}`;
  }
  return { signal, confidence, price, ema9: ema9Now, ema21: ema21Now, rsi, mom1, instantMom, reason };
}

async function executeTrade(pair, signal, confidence, isManual=false, isTurbo=false) {
  if(!exchange) { log('No exchange', 'error'); return; }
  if(!BOT_CONFIG.isRunning && !isManual && !isTurbo) return;
  if(!isManual && !isTurbo && positions.length >= BOT_CONFIG.maxPositions) return;
  if(balance.free < BOT_CONFIG.marginUsdt && balance.free>0 && !isManual) { 
    log(`Skip ${pair}: free $${balance.free.toFixed(2)} < margin $${BOT_CONFIG.marginUsdt}`, 'warn'); return; 
  }
  try {
    const symbol = pair;
    const amountUsdt = Math.max(BOT_CONFIG.marginUsdt, BOT_CONFIG.minUsdt);
    const ticker = await exchange.fetchTicker(symbol);
    const price = ticker.last;
    let qty = (amountUsdt * BOT_CONFIG.leverage) / price;
    try { await exchange.setLeverage(BOT_CONFIG.leverage, symbol); await exchange.setMarginMode('ISOLATED', symbol); } catch(e){}
    const side = signal === 'LONG' ? 'buy' : 'sell';
    const tag = isTurbo ? 'TURBO' : (isManual ? 'MANUAL' : 'ULTRA');
    log(`🚀 ${tag} ${side.toUpperCase()} ${symbol} @${price.toFixed(4)} ${BOT_CONFIG.leverage}x Conf ${confidence}% | TrailAct ${BOT_CONFIG.trailActPercent}%`, 'trade');
    const order = await exchange.createMarketOrder(symbol, side, qty);
    const tpPrice = signal==='LONG' ? price*(1+BOT_CONFIG.tpPercent/100) : price*(1-BOT_CONFIG.tpPercent/100);
    const slPrice = signal==='LONG' ? price*(1-BOT_CONFIG.slPercent/100) : price*(1+BOT_CONFIG.slPercent/100);
    try {
      await exchange.createOrder(symbol, 'TAKE_PROFIT_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: tpPrice, closePosition: true });
      await exchange.createOrder(symbol, 'STOP_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: slPrice, closePosition: true });
    } catch(e){}
    const trade = { id: order.id, pair: symbol, side: signal, entryPrice: price, qty, leverage: BOT_CONFIG.leverage, tp: tpPrice, sl: slPrice, confidence, timestamp: new Date().toISOString(), status: 'OPEN', turbo: isTurbo, manual: isManual };
    tradeHistory.unshift(trade);
    if(tradeHistory.length>100) tradeHistory.pop();
    lastTradeTime = Date.now();
    // init trailing
    trailingMap[symbol] = { active: false, maxPrice: price, minPrice: price, stopPrice: null, entryPrice: price, side: signal, profitPeak: 0 };
    io.emit('newTrade', trade);
    io.emit('tradeHistory', tradeHistory);
    fetchBalance();
  } catch(e){ log(`❌ FAIL ${pair}: ${e.message}`, 'error'); }
}

// ===== TRAILING + AUTO CLOSE LOGIC =====
async function checkAutoCloseAndTrailing() {
  if(!exchange || positions.length===0) return;
  for(const pos of positions) {
    const symbol = pos.symbol;
    const side = pos.side; // long / short
    const mark = pos.markPrice || pos.entryPrice;
    const entry = pos.entryPrice;
    const pnlPercent = pos.percentage || ((mark-entry)/entry*100 * (side==='long'?1:-1) * BOT_CONFIG.leverage);
    const unreal = pos.unrealizedPnl || 0;

    // init trailing if not exists
    if(!trailingMap[symbol]) {
      trailingMap[symbol] = { active: false, maxPrice: mark, minPrice: mark, stopPrice: null, entryPrice: entry, side: side==='long'?'LONG':'SHORT', profitPeak: pnlPercent };
    }
    const trail = trailingMap[symbol];
    if(side==='long') {
      if(mark > trail.maxPrice) trail.maxPrice = mark;
      if(pnlPercent > trail.profitPeak) trail.profitPeak = pnlPercent;
    } else {
      if(mark < trail.minPrice) trail.minPrice = mark;
      if(pnlPercent > trail.profitPeak) trail.profitPeak = pnlPercent;
    }

    // TRAILING ACTIVATION
    if(BOT_CONFIG.trailEnabled && !trail.active && pnlPercent >= BOT_CONFIG.trailActPercent) {
      trail.active = true;
      if(side==='long') {
        trail.stopPrice = trail.maxPrice * (1 - BOT_CONFIG.trailDistancePercent/100);
      } else {
        trail.stopPrice = trail.minPrice * (1 + BOT_CONFIG.trailDistancePercent/100);
      }
      log(`🟢 TRAIL ACT ${symbol} ${side.toUpperCase()} PnL ${pnlPercent.toFixed(3)}% >= Act ${BOT_CONFIG.trailActPercent}% → Stop ${trail.stopPrice.toFixed(4)}`, 'profit');
    }

    // UPDATE TRAILING STOP if active and price goes more profitable
    if(trail.active) {
      if(side==='long') {
        const newStop = trail.maxPrice * (1 - BOT_CONFIG.trailDistancePercent/100);
        if(newStop > trail.stopPrice) trail.stopPrice = newStop;
        // check hit
        if(mark <= trail.stopPrice) {
          try {
            await exchange.createMarketOrder(symbol, 'sell', Math.abs(pos.contracts), undefined, { reduceOnly: true });
            const profit = unreal;
            totalProfit += profit;
            if(profit>0) winCount++; else lossCount++;
            log(`✅ TRAIL CLOSE ${symbol} LONG @${mark.toFixed(4)} Peak ${trail.profitPeak.toFixed(3)}% Profit $${profit.toFixed(3)} TrailStop ${trail.stopPrice.toFixed(4)}`, 'profit');
            delete trailingMap[symbol];
            tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TRAIL'; t.exitPrice=mark; t.profit=profit; } });
          } catch(e){ log(`Trail close fail ${symbol}: ${e.message}`, 'error'); }
          continue;
        }
      } else { // short
        const newStop = trail.minPrice * (1 + BOT_CONFIG.trailDistancePercent/100);
        if(newStop < trail.stopPrice) trail.stopPrice = newStop;
        if(mark >= trail.stopPrice) {
          try {
            await exchange.createMarketOrder(symbol, 'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
            const profit = unreal;
            totalProfit += profit;
            if(profit>0) winCount++; else lossCount++;
            log(`✅ TRAIL CLOSE ${symbol} SHORT @${mark.toFixed(4)} Peak ${trail.profitPeak.toFixed(3)}% Profit $${profit.toFixed(3)}`, 'profit');
            delete trailingMap[symbol];
            tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TRAIL'; t.exitPrice=mark; t.profit=profit; } });
          } catch(e){ log(`Trail close fail ${symbol}: ${e.message}`, 'error'); }
          continue;
        }
      }
    }

    // NORMAL TP AUTO CLOSE (if trailing not active)
    if(!trail.active && (pnlPercent >= BOT_CONFIG.tpPercent || unreal >= (BOT_CONFIG.marginUsdt * BOT_CONFIG.tpPercent/100))) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal;
        totalProfit += profit;
        if(profit>0) winCount++; else lossCount++;
        log(`✅ TP CLOSE ${symbol} ${pnlPercent.toFixed(3)}% >= TP ${BOT_CONFIG.tpPercent}% Profit $${profit.toFixed(3)}`, 'profit');
        delete trailingMap[symbol];
        tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TP'; t.exitPrice=mark; t.profit=profit; } });
      } catch(e){ log(`TP close fail ${symbol}: ${e.message}`, 'error'); }
    }

    // SL
    if(pnlPercent <= -BOT_CONFIG.slPercent) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal;
        totalProfit += profit;
        lossCount++;
        log(`🛑 SL CLOSE ${symbol} ${pnlPercent.toFixed(3)}% <= -${BOT_CONFIG.slPercent}% Loss $${profit.toFixed(3)}`, 'error');
        delete trailingMap[symbol];
        tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_SL'; t.exitPrice=mark; t.profit=profit; } });
      } catch(e){}
    }
  }
}

let scanning = false;
async function startScanner() {
  if(!exchange) return;
  if(scanning) return;
  scanning = true;
  log(`⚡ ULTRA V4.1 TRAIL READY - Scan ${BOT_CONFIG.scanIntervalMs}ms - TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% TrailAct ${BOT_CONFIG.trailActPercent}% TrailDist ${BOT_CONFIG.trailDistancePercent}%`, 'success');
  setInterval(checkAutoCloseAndTrailing, 1200);
  setInterval(async () => {
    if(!BOT_CONFIG.isRunning) return;
    if(BOT_CONFIG.turboMode && Date.now() - lastTradeTime > 45000 && positions.length < BOT_CONFIG.maxPositions) {
      const volatile = TOP_PAIRS[Math.floor(Math.random()*6)];
      try {
        const candles = await exchange.fetchOHLCV(volatile, '1m', undefined, 30);
        const analysis = calculateUltraSignal(candles, volatile);
        const forcedSide = analysis.mom1 >=0 ? 'LONG' : 'SHORT';
        log(`🔥 TURBO FORCE ${forcedSide} ${volatile}`, 'turbo');
        await executeTrade(volatile, forcedSide, 55, false, true);
      } catch(e){}
    }
    for(let i=0;i<TOP_PAIRS.length;i++){
      const pair = TOP_PAIRS[i];
      try {
        const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 35);
        const analysis = calculateUltraSignal(candles, pair);
        marketData[pair] = { ...analysis, pair, lastUpdate: Date.now() };
        if(analysis.signal !== 'HOLD' && analysis.confidence >= BOT_CONFIG.confidenceThreshold){
          log(`⚡ ${pair} ${analysis.signal} ${analysis.confidence}% ${analysis.reason}`, 'signal');
          await executeTrade(pair, analysis.signal, analysis.confidence);
          if(positions.length >= BOT_CONFIG.maxPositions) break;
        }
      } catch(e){ }
      await new Promise(r=>setTimeout(r, 40));
    }
    io.emit('marketData', marketData);
    io.emit('scanLogs', scanLogs);
    io.emit('stats', { totalProfit, winCount, lossCount, lastTradeAgo: Math.floor((Date.now()-lastTradeTime)/1000), trailingCount: Object.keys(trailingMap).length });
  }, BOT_CONFIG.scanIntervalMs);
}

app.get('/api/config', (req,res)=> res.json({ config: BOT_CONFIG, mode: MODE, pairs: TOP_PAIRS, balance, logs: scanLogs, stats: { totalProfit, winCount, lossCount }, trailing: trailingMap }));
app.post('/api/config', (req,res)=>{
  const { leverage, marginUsdt, tpPercent, slPercent, maxPositions, minUsdt, confidenceThreshold, ultraMode, turboMode, trailActPercent, trailDistancePercent, trailEnabled } = req.body;
  if(leverage) BOT_CONFIG.leverage = parseInt(leverage);
  if(marginUsdt) BOT_CONFIG.marginUsdt = parseFloat(marginUsdt);
  if(tpPercent) BOT_CONFIG.tpPercent = parseFloat(tpPercent);
  if(slPercent) BOT_CONFIG.slPercent = parseFloat(slPercent);
  if(maxPositions) BOT_CONFIG.maxPositions = parseInt(maxPositions);
  if(minUsdt) BOT_CONFIG.minUsdt = parseFloat(minUsdt);
  if(confidenceThreshold) BOT_CONFIG.confidenceThreshold = parseInt(confidenceThreshold);
  if(ultraMode!==undefined) BOT_CONFIG.ultraMode = ultraMode;
  if(turboMode!==undefined) BOT_CONFIG.turboMode = turboMode;
  if(trailActPercent!==undefined) BOT_CONFIG.trailActPercent = parseFloat(trailActPercent);
  if(trailDistancePercent!==undefined) BOT_CONFIG.trailDistancePercent = parseFloat(trailDistancePercent);
  if(trailEnabled!==undefined) BOT_CONFIG.trailEnabled = trailEnabled;
  io.emit('config', BOT_CONFIG);
  log(`Config V4.1 updated TrailAct ${BOT_CONFIG.trailActPercent}% TrailDist ${BOT_CONFIG.trailDistancePercent}%`, 'info');
  res.json({ success: true, config: BOT_CONFIG });
});

app.post('/api/test-trade', async (req,res)=>{
  const { pair, side } = req.body;
  await executeTrade(pair||'BTC/USDT', side||'LONG', 99, true);
  res.json({ success: true });
});

app.post('/api/instant-trade', async (req,res)=>{
  log('⚡ INSTANT TRADE', 'turbo');
  let best = null, bestMom = 0;
  for(let pair of TOP_PAIRS.slice(0,10)){
    try {
      const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 10);
      const closes = candles.map(c=>c[4]);
      const mom = ((closes[closes.length-1]-closes[closes.length-2])/closes[closes.length-2])*100;
      if(Math.abs(mom) > Math.abs(bestMom)) { bestMom = mom; best = { pair, mom, side: mom>0?'LONG':'SHORT' }; }
    } catch {}
  }
  if(best) { await executeTrade(best.pair, best.side, 75, true); res.json({ success: true, picked: best }); }
  else { await executeTrade('BTC/USDT','LONG',75,true); res.json({ success: true }); }
});

app.post('/api/bot/:action', (req,res)=>{
  const { action } = req.params;
  if(action==='start'){ BOT_CONFIG.isRunning = true; lastTradeTime = Date.now(); io.emit('botStatus','RUNNING'); log(`🚀 ULTRA V4.1 TRAIL STARTED TrailAct ${BOT_CONFIG.trailActPercent}%`, 'success'); }
  if(action==='stop'){ BOT_CONFIG.isRunning = false; io.emit('botStatus','STOPPED'); log('BOT STOPPED', 'warn'); }
  if(action==='emergency'){
    BOT_CONFIG.isRunning = false;
    (async()=>{ for(const pos of positions){ try{ await exchange.createMarketOrder(pos.symbol, pos.side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true }); }catch(e){} } trailingMap={}; })();
    io.emit('botStatus','EMERGENCY_STOP');
    log('EMERGENCY STOP', 'error');
  }
  res.json({ success: true, isRunning: BOT_CONFIG.isRunning });
});

app.get('/api/balance', async (req,res)=>{ await fetchBalance(); res.json(balance); });
app.get('/api/positions', (req,res)=> res.json(positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null}))));
app.get('/api/history', (req,res)=> res.json(tradeHistory));
app.get('/api/logs', (req,res)=> res.json(scanLogs));

app.get('/', (req,res)=>{
  const publicPath = path.join(__dirname, 'public', 'index.html');
  const rootPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(publicPath)) return res.sendFile(publicPath);
  else if (fs.existsSync(rootPath)) return res.sendFile(rootPath);
  else return res.send('<h1>Bot Running - Add index.html</h1>');
});

io.on('connection', (socket)=>{
  socket.emit('config', BOT_CONFIG);
  socket.emit('balance', { ...balance, totalProfit, winCount, lossCount });
  socket.emit('marketData', marketData);
  socket.emit('tradeHistory', tradeHistory);
  socket.emit('positions', positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null})));
  socket.emit('scanLogs', scanLogs);
  socket.emit('botStatus', BOT_CONFIG.isRunning?'RUNNING':'STOPPED');
  socket.emit('trailing', trailingMap);
  const balInterval = setInterval(fetchBalance, 2500);
  const trailInterval = setInterval(()=> io.emit('trailing', trailingMap), 1500);
  socket.on('disconnect', ()=> { clearInterval(balInterval); clearInterval(trailInterval); });
});

if(process.env.BINANCE_API_KEY && process.env.BINANCE_API_SECRET){
  initExchange(process.env.BINANCE_API_KEY, process.env.BINANCE_API_SECRET, MODE);
  fetchBalance().then(()=>{ log(`✅ Auto connected ${MODE} V4.1 TRAIL READY`, 'success'); startScanner(); }).catch(e=>log('Auto connect fail '+e.message, 'error'));
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, ()=> console.log(`Bot V4.1 ULTRA TRAIL running ${PORT} Mode:${MODE}`));
