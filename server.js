
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

// V8 ULTRA SPEED - User wants speed, not big profit - 30 sec open/close
let BOT_CONFIG = {
  leverage: 10,
  marginUsdt: 4,
  tpPercent: 0.35,
  slPercent: 0.35,
  maxPositions: 2,
  minUsdt: 1,
  isRunning: false,
  confidenceThreshold: 60,
  ultraMode: true,
  turboMode: true,
  scanIntervalMs: 700,
  trailActPercent: 0.18,
  trailDistancePercent: 0.10,
  trailEnabled: true,
  maxDailyLoss: 4,
  cooldownAfterLossSec: 8,
  cooldownAfterWinSec: 3,
  speedMode: true
};

// SPEED PAIRS - 6 liquid pairs only - fast but not meme manipulation
const TOP_PAIRS = ['BTC/USDT','ETH/USDT','SOL/USDT','BNB/USDT','AVAX/USDT','LINK/USDT'];

let exchange = null;
let balance = { total: 0, free: 0, pnl: 0, status: 'disconnected', unrealizedPnl: 0 };
let positions = [];
let tradeHistory = [];
let marketData = {};
let scanLogs = [];
let lastTradeTime = 0;
let lastLossTime = 0;
let lastWinTime = 0;
let priceCache = {};
let totalProfit = 0;
let winCount = 0, lossCount = 0;
let trailingMap = {};
let todayProfit = 0;
let todayTrades = [];
let consecutiveLosses = 0;
let speedStats = { scanned: 0, micro: 0, tradesPerMin: 0, lastMinTrades: [], avgCloseSec: 0, closeTimes: [] };

function log(msg, type='info') {
  const entry = { time: new Date().toLocaleTimeString(), msg, type };
  scanLogs.unshift(entry);
  if(scanLogs.length>150) scanLogs.pop();
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
  } catch(e) { balance.status = 'balance_err'; }
  try {
    let poss = [];
    try { poss = await exchange.fetchPositions(); } 
    catch(e) {
      if(e.message.includes('positionRisk') || e.message.includes('-5000')) {
        try {
          const raw = await exchange.fapiPrivateV2GetPositionRisk().catch(()=>null);
          if(raw) {
            poss = raw.filter(p=> parseFloat(p.positionAmt) !== 0).map(p=> ({
              symbol: p.symbol.replace('USDT','/USDT'),
              side: parseFloat(p.positionAmt) > 0 ? 'long' : 'short',
              contracts: Math.abs(parseFloat(p.positionAmt)),
              entryPrice: parseFloat(p.entryPrice),
              markPrice: parseFloat(p.markPrice),
              unrealizedPnl: parseFloat(p.unRealizedProfit),
              percentage: parseFloat(p.unRealizedProfit) / (parseFloat(p.entryPrice)*Math.abs(parseFloat(p.positionAmt))) *100 * BOT_CONFIG.leverage || 0,
              leverage: parseFloat(p.leverage)
            }));
          }
        } catch { poss = []; }
      }
    }
    let totalPnl = 0;
    poss.forEach(p => { if(p.unrealizedPnl) totalPnl += p.unrealizedPnl; });
    balance.pnl = totalPnl; balance.unrealizedPnl = totalPnl;
    positions = poss.filter(p => Math.abs(parseFloat(p.contracts || p.positionAmt || 0)) > 0);
    const openSymbols = positions.map(p=>p.symbol);
    Object.keys(trailingMap).forEach(sym => { if(!openSymbols.includes(sym)) delete trailingMap[sym]; });
    const now = Date.now();
    speedStats.lastMinTrades = speedStats.lastMinTrades.filter(t=> now - t < 60000);
    speedStats.tradesPerMin = speedStats.lastMinTrades.length;
    if(speedStats.closeTimes.length > 0) {
      speedStats.avgCloseSec = (speedStats.closeTimes.reduce((a,b)=>a+b,0) / speedStats.closeTimes.length).toFixed(1);
    }
    io.emit('balance', { ...balance, totalProfit, winCount, lossCount, totalTrades: tradeHistory.length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: totalPnl, consecutiveLosses, speedStats });
    io.emit('positions', positions.map(p=> ({...p, trailing: trailingMap[p.symbol] || null})) );
  } catch(e) {
    io.emit('balance', { ...balance, status: 'connected', totalProfit, winCount, lossCount, totalTrades: tradeHistory.length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, speedStats });
    io.emit('positions', positions.map(p=> ({...p, trailing: trailingMap[p.symbol] || null})) );
  }
}

function ema(arr, period) {
  const k = 2/(period+1);
  let e=[arr[0]];
  for(let i=1;i<arr.length;i++) e.push(arr[i]*k + e[i-1]*(1-k));
  return e;
}
function rsiCalc(closes) {
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
}

// V8 ULTRA SPEED - User: loku profit oni na, speed eken trade karanna oni
// How to make trade close in 30 sec?
// - TP 0.35% very small => price needs to move only 0.035% with 10x leverage for 0.35% PNL
// - Trail act 0.18% => after 0.18% profit, trail locks
// - Micro momentum: if price moves 0.08% in 10 sec, enter, it will hit 0.35% in next 20-40 sec in volatile market
// - No 5m trend filter (for speed), only 1m EMA9>21 micro trend
// - 6 pairs, 0.7s scan, 2 pos parallel, cooldown 8s/3s = 2-3 trades/min
function calculateSpeedSignal(candles, pair) {
  speedStats.scanned++;
  if(candles.length < 20) return { signal: 'HOLD', confidence: 0, reason: 'wait' };
  const closes = candles.map(c => c[4]);
  const volumes = candles.map(c => c[5]);
  const last = closes.length-1;
  const price = closes[last];
  const prev1 = closes[last-1];
  const prev2 = closes[Math.max(0,last-2)];
  
  const mom1 = ((price - prev1)/prev1)*100;
  const mom2 = ((price - prev2)/prev2)*100;
  const vol = volumes[last];
  const volAvg = volumes.slice(-10).reduce((a,b)=>a+b,0)/10;
  const volRatio = vol / (volAvg || 1);
  
  const e9 = ema(closes,9); const e21 = ema(closes,21);
  const e9Now = e9[last], e21Now = e21[last];
  const rsi = rsiCalc(closes);
  
  // Micro momentum cache - 10 sec price change
  const cached = priceCache[pair];
  let microMom = 0;
  let secAgo = 0;
  if(cached) {
    microMom = ((price - cached.price)/cached.price)*100;
    secAgo = (Date.now() - cached.time)/1000;
  }
  // Update cache every 3 sec
  if(!cached || Date.now() - cached.time > 3000) {
    priceCache[pair] = { price, time: Date.now() };
  }
  if(Math.abs(microMom) > 0.06) speedStats.micro++;

  const emaGap = Math.abs(e9Now - e21Now) / price * 100;
  const trendUp = e9Now > e21Now;
  const trendDown = e9Now < e21Now;

  let signal='HOLD', confidence=0, reason='';

  // SPEED LONG - micro pump 0.08% + EMA up + RSI 40-65 + vol
  if(trendUp && rsi >= 40 && rsi <= 65 && volRatio > 0.95) {
    if((mom1 > 0.06 && mom2 > 0.08) || microMom > 0.08) {
      confidence = 62 + Math.min(18, Math.abs(mom1)*30 + volRatio*3 + emaGap*80);
      if(rsi >= 45 && rsi <= 58) confidence+=5;
      if(volRatio > 1.3) confidence+=3;
      signal='LONG';
      reason=`SPEED LONG MOM ${mom1.toFixed(3)}% 2M ${mom2.toFixed(3)}% micro ${microMom.toFixed(3)}% ${secAgo.toFixed(0)}s RSI ${rsi.toFixed(0)} VOL x${volRatio.toFixed(1)} GAP ${emaGap.toFixed(3)}%`;
    }
  }
  // SPEED SHORT
  if(signal==='HOLD' && trendDown && rsi >= 40 && rsi <= 65 && volRatio > 0.95) {
    if((mom1 < -0.06 && mom2 < -0.08) || microMom < -0.08) {
      confidence = 62 + Math.min(18, Math.abs(mom1)*30 + volRatio*3 + emaGap*80);
      if(rsi >= 42 && rsi <= 60) confidence+=5;
      if(volRatio > 1.3) confidence+=3;
      signal='SHORT';
      reason=`SPEED SHORT MOM ${mom1.toFixed(3)}% micro ${microMom.toFixed(3)}% RSI ${rsi.toFixed(0)} VOL x${volRatio.toFixed(1)}`;
    }
  }
  if(signal==='HOLD') {
    reason=`WAIT SPEED MOM ${mom1.toFixed(3)}% micro ${microMom.toFixed(3)}% RSI ${rsi.toFixed(0)} GAP ${emaGap.toFixed(3)}%`;
  }

  return { signal, confidence: Math.min(86, confidence), price, ema9: e9Now, ema21: e21Now, rsi, mom1, mom2, microMom, emaGap, volRatio, reason };
}

async function executeTrade(pair, signal, confidence, isManual=false) {
  if(!exchange) return;
  if(!BOT_CONFIG.isRunning && !isManual) return;
  if(!isManual && positions.length >= BOT_CONFIG.maxPositions) return;
  
  const now = Date.now();
  if(!isManual) {
    if(now - lastLossTime < BOT_CONFIG.cooldownAfterLossSec*1000) return;
    if(now - lastWinTime < BOT_CONFIG.cooldownAfterWinSec*1000) return;
  }
  if(todayProfit <= -BOT_CONFIG.maxDailyLoss) {
    log(`🛑 DAILY LOSS $${BOT_CONFIG.maxDailyLoss} STOP - Protect`, 'error');
    BOT_CONFIG.isRunning = false;
    io.emit('botStatus','STOPPED_DAILY_LOSS');
    return;
  }
  if(consecutiveLosses >= 4 && !isManual) {
    log(`⚠️ 4 losses, threshold 60% -> 68% for safety`, 'warn');
    BOT_CONFIG.confidenceThreshold = 68;
  }

  try {
    const symbol = pair;
    const amountUsdt = BOT_CONFIG.marginUsdt;
    const ticker = await exchange.fetchTicker(symbol);
    const price = ticker.last;
    let qty = (amountUsdt * BOT_CONFIG.leverage) / price;
    try { await exchange.setLeverage(BOT_CONFIG.leverage, symbol); await exchange.setMarginMode('ISOLATED', symbol); } catch(e){}
    const side = signal === 'LONG' ? 'buy' : 'sell';
    const entryTime = Date.now();
    log(`⚡ SPEED ${side.toUpperCase()} ${symbol} @${price.toFixed(4)} ${BOT_CONFIG.leverage}x TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% Conf ${confidence}% - Close in 30 sec target`, 'trade');
    const order = await exchange.createMarketOrder(symbol, side, qty);
    const tpPrice = signal==='LONG' ? price*(1+BOT_CONFIG.tpPercent/100) : price*(1-BOT_CONFIG.tpPercent/100);
    const slPrice = signal==='LONG' ? price*(1-BOT_CONFIG.slPercent/100) : price*(1+BOT_CONFIG.slPercent/100);
    try {
      await exchange.createOrder(symbol, 'TAKE_PROFIT_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: tpPrice, closePosition: true });
      await exchange.createOrder(symbol, 'STOP_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: slPrice, closePosition: true });
    } catch(e){}
    const trade = { id: order.id, pair: symbol, side: signal, entryPrice: price, qty, leverage: BOT_CONFIG.leverage, tp: tpPrice, sl: slPrice, confidence, timestamp: new Date().toISOString(), status: 'OPEN', pnl: 0, entryTime, speed: true };
    tradeHistory.unshift(trade); if(tradeHistory.length>150) tradeHistory.pop();
    todayTrades.unshift(trade);
    lastTradeTime = now;
    speedStats.lastMinTrades.push(now);
    trailingMap[symbol] = { active: false, maxPrice: price, minPrice: price, stopPrice: null, entryPrice: price, side: signal, profitPeak: 0, entryTime };
    io.emit('newTrade', trade); io.emit('tradeHistory', tradeHistory);
    fetchBalance();
  } catch(e){ log(`❌ FAIL ${pair}: ${e.message.slice(0,60)}`, 'error'); }
}

async function checkAutoCloseAndTrailing() {
  if(!exchange || positions.length===0) return;
  for(const pos of positions) {
    const symbol = pos.symbol;
    const side = pos.side;
    const mark = pos.markPrice || pos.entryPrice;
    const entry = pos.entryPrice;
    const pnlPercent = pos.percentage || ((mark-entry)/entry*100 * (side==='long'?1:-1) * BOT_CONFIG.leverage);
    const unreal = pos.unrealizedPnl || 0;
    if(!trailingMap[symbol]) trailingMap[symbol] = { active: false, maxPrice: mark, minPrice: mark, stopPrice: null, entryPrice: entry, side: side==='long'?'LONG':'SHORT', profitPeak: pnlPercent, entryTime: Date.now() };
    const trail = trailingMap[symbol];
    if(side==='long') { if(mark > trail.maxPrice) trail.maxPrice = mark; if(pnlPercent > trail.profitPeak) trail.profitPeak = pnlPercent; }
    else { if(mark < trail.minPrice) trail.minPrice = mark; if(pnlPercent > trail.profitPeak) trail.profitPeak = pnlPercent; }

    // ULTRA FAST TRAIL - Act at 0.18% (vs 0.50%), lock profit in 20-40 sec
    if(BOT_CONFIG.trailEnabled && !trail.active && pnlPercent >= BOT_CONFIG.trailActPercent) {
      trail.active = true;
      if(side==='long') trail.stopPrice = trail.maxPrice * (1 - BOT_CONFIG.trailDistancePercent/100);
      else trail.stopPrice = trail.minPrice * (1 + BOT_CONFIG.trailDistancePercent/100);
      log(`🟢 SPEED TRAIL ACT ${symbol} ${pnlPercent.toFixed(3)}% - Lock in 10 sec`, 'profit');
    }
    if(trail.active) {
      if(side==='long') {
        const newStop = trail.maxPrice * (1 - BOT_CONFIG.trailDistancePercent/100);
        if(newStop > trail.stopPrice) trail.stopPrice = newStop;
        if(mark <= trail.stopPrice) {
          try {
            await exchange.createMarketOrder(symbol, 'sell', Math.abs(pos.contracts), undefined, { reduceOnly: true });
            const profit = unreal; totalProfit += profit; todayProfit += profit;
            const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
            speedStats.closeTimes.push(holdSec); if(speedStats.closeTimes.length>20) speedStats.closeTimes.shift();
            if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=60; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
            log(`✅ SPEED TRAIL CLOSE ${symbol} ${holdSec.toFixed(0)}s Peak ${trail.profitPeak.toFixed(3)}% $${profit.toFixed(3)}`, 'profit');
            delete trailingMap[symbol];
            tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TRAIL'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
          } catch(e){}
          continue;
        }
      } else {
        const newStop = trail.minPrice * (1 + BOT_CONFIG.trailDistancePercent/100);
        if(newStop < trail.stopPrice) trail.stopPrice = newStop;
        if(mark >= trail.stopPrice) {
          try {
            await exchange.createMarketOrder(symbol, 'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
            const profit = unreal; totalProfit += profit; todayProfit += profit;
            const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
            speedStats.closeTimes.push(holdSec); if(speedStats.closeTimes.length>20) speedStats.closeTimes.shift();
            if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=60; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
            log(`✅ SPEED TRAIL CLOSE SHORT ${symbol} ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'profit');
            delete trailingMap[symbol];
            tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TRAIL'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
          } catch(e){}
          continue;
        }
      }
    }
    // TP 0.35% very small - close in 30 sec target
    if(!trail.active && pnlPercent >= BOT_CONFIG.tpPercent) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal; totalProfit += profit; todayProfit += profit;
        const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
        speedStats.closeTimes.push(holdSec); if(speedStats.closeTimes.length>20) speedStats.closeTimes.shift();
        if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=60; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
        log(`✅ SPEED TP CLOSE ${symbol} ${pnlPercent.toFixed(3)}% ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'profit');
        delete trailingMap[symbol];
        tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TP'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
      } catch(e){}
    }
    if(pnlPercent <= -BOT_CONFIG.slPercent) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal; totalProfit += profit; todayProfit += profit; lossCount++; consecutiveLosses++; lastLossTime=Date.now();
        const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
        speedStats.closeTimes.push(holdSec); if(speedStats.closeTimes.length>20) speedStats.closeTimes.shift();
        log(`🛑 SPEED SL CLOSE ${symbol} ${pnlPercent.toFixed(3)}% ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'error');
        delete trailingMap[symbol];
        tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_SL'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
      } catch(e){}
    }
  }
}

let scanning = false;
async function startScanner() {
  if(!exchange) return;
  if(scanning) return;
  scanning = true;
  log(`⚡ ULTRA SPEED V8 STARTED - TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% 0.7s scan - 30 sec close target`, 'success');
  log(`💨 USER WANTS SPEED NOT BIG PROFIT: Small $0.10-$0.20 profit but close in 30 sec, 2-3 trades/min`, 'info');
  log(`⚠️ Speed = 60% WR needed, net 0.27% profit vs -0.43% loss, 10 trades = $1 profit if 60% WR`, 'warn');
  setInterval(checkAutoCloseAndTrailing, 500);
  setInterval(async () => {
    if(!BOT_CONFIG.isRunning) return;
    for(let i=0;i<TOP_PAIRS.length;i++){
      const pair = TOP_PAIRS[i];
      try {
        const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 30);
        const analysis = calculateSpeedSignal(candles, pair);
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
    const now = Date.now();
    speedStats.lastMinTrades = speedStats.lastMinTrades.filter(t=> now - t < 60000);
    speedStats.tradesPerMin = speedStats.lastMinTrades.length;
    if(speedStats.closeTimes.length>0) speedStats.avgCloseSec = (speedStats.closeTimes.reduce((a,b)=>a+b,0)/speedStats.closeTimes.length).toFixed(1);
    io.emit('stats', { totalProfit, winCount, lossCount, lastTradeAgo: Math.floor((Date.now()-lastTradeTime)/1000), trailingCount: Object.keys(trailingMap).length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, speedStats });
  }, BOT_CONFIG.scanIntervalMs);
}

app.get('/api/config', (req,res)=> res.json({ config: BOT_CONFIG, mode: MODE, pairs: TOP_PAIRS, balance, logs: scanLogs, stats: { totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, speedStats }, trailing: trailingMap }));
app.post('/api/config', (req,res)=>{
  const { leverage, marginUsdt, tpPercent, slPercent, maxPositions, minUsdt, confidenceThreshold, trailActPercent, trailDistancePercent, trailEnabled, maxDailyLoss, cooldownAfterLossSec, cooldownAfterWinSec } = req.body;
  if(leverage) BOT_CONFIG.leverage = parseInt(leverage);
  if(marginUsdt) BOT_CONFIG.marginUsdt = parseFloat(marginUsdt);
  if(tpPercent) BOT_CONFIG.tpPercent = parseFloat(tpPercent);
  if(slPercent) BOT_CONFIG.slPercent = parseFloat(slPercent);
  if(maxPositions) BOT_CONFIG.maxPositions = parseInt(maxPositions);
  if(minUsdt) BOT_CONFIG.minUsdt = parseFloat(minUsdt);
  if(confidenceThreshold) BOT_CONFIG.confidenceThreshold = parseInt(confidenceThreshold);
  if(trailActPercent!==undefined) BOT_CONFIG.trailActPercent = parseFloat(trailActPercent);
  if(trailDistancePercent!==undefined) BOT_CONFIG.trailDistancePercent = parseFloat(trailDistancePercent);
  if(trailEnabled!==undefined) BOT_CONFIG.trailEnabled = trailEnabled;
  if(maxDailyLoss!==undefined) BOT_CONFIG.maxDailyLoss = parseFloat(maxDailyLoss);
  if(cooldownAfterLossSec!==undefined) BOT_CONFIG.cooldownAfterLossSec = parseInt(cooldownAfterLossSec);
  if(cooldownAfterWinSec!==undefined) BOT_CONFIG.cooldownAfterWinSec = parseInt(cooldownAfterWinSec);
  io.emit('config', BOT_CONFIG);
  log(`Config V8 SPEED TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% Thr ${BOT_CONFIG.confidenceThreshold}%`, 'info');
  res.json({ success: true, config: BOT_CONFIG });
});

app.post('/api/test-trade', async (req,res)=>{ await executeTrade(req.body.pair||'BTC/USDT', req.body.side||'LONG', 99, true); res.json({ success: true }); });
app.post('/api/instant-trade', async (req,res)=>{
  log('⚡ SPEED INSTANT - 6 pairs 0.7s', 'info');
  let best = null, bestConf = 0;
  for(let pair of TOP_PAIRS){
    try {
      const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 30);
      const analysis = calculateSpeedSignal(candles, pair);
      if(analysis.confidence > bestConf && analysis.signal !== 'HOLD') { bestConf = analysis.confidence; best = { pair, side: analysis.signal, conf: analysis.confidence, reason: analysis.reason }; }
    } catch {}
  }
  if(best && bestConf >= 55) { await executeTrade(best.pair, best.side, best.conf, true); res.json({ success: true, picked: best }); }
  else { res.json({ success: false, msg: best ? `Best ${best.pair} ${best.conf}% but need 60%` : 'No speed momentum now - wait 0.08% micro pump in 10 sec' }); }
});

app.post('/api/bot/:action', (req,res)=>{
  const { action } = req.params;
  if(action==='start'){ BOT_CONFIG.isRunning = true; lastTradeTime = Date.now(); speedStats={ scanned:0, micro:0, tradesPerMin:0, lastMinTrades:[], avgCloseSec:0, closeTimes:[] }; io.emit('botStatus','RUNNING_SPEED'); log(`🚀 SPEED V8 STARTED - 30 sec close target - 6 pairs 0.7s scan`, 'success'); }
  if(action==='stop'){ BOT_CONFIG.isRunning = false; io.emit('botStatus','STOPPED'); log('SPEED STOPPED', 'warn'); }
  if(action==='emergency'){
    BOT_CONFIG.isRunning = false;
    (async()=>{ for(const pos of positions){ try{ await exchange.createMarketOrder(pos.symbol, pos.side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true }); }catch(e){} } trailingMap={}; })();
    io.emit('botStatus','EMERGENCY_STOP'); log('EMERGENCY STOP', 'error');
  }
  if(action==='reset-stats'){
    totalProfit=0; winCount=0; lossCount=0; todayProfit=0; todayTrades=[]; tradeHistory=[]; trailingMap={}; consecutiveLosses=0; speedStats={ scanned:0, micro:0, tradesPerMin:0, lastMinTrades:[], avgCloseSec:0, closeTimes:[] };
    log('Stats reset - Speed fresh', 'warn'); io.emit('tradeHistory', tradeHistory);
  }
  res.json({ success: true, isRunning: BOT_CONFIG.isRunning });
});

app.get('/api/balance', async (req,res)=>{ await fetchBalance(); res.json({...balance, totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, speedStats }); });
app.get('/api/positions', (req,res)=> res.json(positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null}))));
app.get('/api/history', (req,res)=> res.json(tradeHistory));
app.get('/api/logs', (req,res)=> res.json(scanLogs));
app.get('/api/stats', (req,res)=> res.json({ totalProfit, winCount, lossCount, todayProfit, todayTrades, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, speedStats }));

app.get('/', (req,res)=>{
  const publicPath = path.join(__dirname, 'public', 'index.html');
  const rootPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(publicPath)) return res.sendFile(publicPath);
  else if (fs.existsSync(rootPath)) return res.sendFile(rootPath);
  else return res.send('<h1>Speed Running</h1>');
});

io.on('connection', (socket)=>{
  socket.emit('config', BOT_CONFIG);
  socket.emit('balance', { ...balance, totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, speedStats });
  socket.emit('marketData', marketData);
  socket.emit('tradeHistory', tradeHistory);
  socket.emit('positions', positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null})));
  socket.emit('scanLogs', scanLogs);
  socket.emit('botStatus', BOT_CONFIG.isRunning?'RUNNING_SPEED':'STOPPED');
  socket.emit('trailing', trailingMap);
  socket.emit('stats', { totalProfit, winCount, lossCount, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, speedStats });
  const balInterval = setInterval(fetchBalance, 2000);
  const statsInterval = setInterval(()=> { 
    const now = Date.now();
    speedStats.lastMinTrades = speedStats.lastMinTrades.filter(t=> now - t < 60000);
    speedStats.tradesPerMin = speedStats.lastMinTrades.length;
    if(speedStats.closeTimes.length>0) speedStats.avgCloseSec = (speedStats.closeTimes.reduce((a,b)=>a+b,0)/speedStats.closeTimes.length).toFixed(1);
    io.emit('stats', { totalProfit, winCount, lossCount, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, trailingCount: Object.keys(trailingMap).length, unrealizedPnl: balance.pnl, consecutiveLosses, speedStats, lastTradeAgo: Math.floor((Date.now()-lastTradeTime)/1000) }); 
  }, 1000);
  socket.on('disconnect', ()=> { clearInterval(balInterval); clearInterval(statsInterval); });
});

if(process.env.BINANCE_API_KEY && process.env.BINANCE_API_SECRET){
  initExchange(process.env.BINANCE_API_KEY, process.env.BINANCE_API_SECRET, MODE);
  fetchBalance().then(()=>{ log(`✅ Connected ${MODE} SPEED V8 READY - 30 sec close`, 'success'); startScanner(); }).catch(e=>log('Auto connect fail '+e.message, 'error'));
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, ()=> console.log(`Bot V8 SPEED ${PORT} Mode:${MODE}`));
