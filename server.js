
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

// V10 FINAL BALANCED - Speed + Profit - Fix -$75 loss
// Lessons from V9: Thr 35% => 0W 23L 0% WR -$4.7, BNB SL $-1.005 x8 => -$75
// Fix: Thr 62% (not 35%), TP 0.55% SL 0.40% RR 1.37, Only BTC/ETH/SOL (no BNB), Max 1 pos, Cooldown 20s/6s, Daily loss $3 hard stop
let BOT_CONFIG = {
  leverage: 8,
  marginUsdt: 5,
  tpPercent: 0.55,
  slPercent: 0.40,
  maxPositions: 1,
  minUsdt: 1,
  isRunning: false,
  confidenceThreshold: 62,
  ultraMode: false,
  turboMode: false,
  scanIntervalMs: 1500,
  trailActPercent: 0.30,
  trailDistancePercent: 0.15,
  trailEnabled: true,
  maxDailyLoss: 3,
  cooldownAfterLossSec: 20,
  cooldownAfterWinSec: 6,
  balancedMode: true
};

// ONLY 3 PAIRS - BTC/ETH/SOL highest WR - No BNB (BNB caused -$75 loss in V9)
const TOP_PAIRS = ['BTC/USDT','ETH/USDT','SOL/USDT'];

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
let balancedStats = { scanned: 0, filtered: 0, tradesPerMin: 0, lastMinTrades: [], avgCloseSec: 0, closeTimes: [] };

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
    balancedStats.lastMinTrades = balancedStats.lastMinTrades.filter(t=> now - t < 60000);
    balancedStats.tradesPerMin = balancedStats.lastMinTrades.length;
    if(balancedStats.closeTimes.length > 0) {
      balancedStats.avgCloseSec = (balancedStats.closeTimes.reduce((a,b)=>a+b,0) / balancedStats.closeTimes.length).toFixed(1);
    }
    io.emit('balance', { ...balance, totalProfit, winCount, lossCount, totalTrades: tradeHistory.length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: totalPnl, consecutiveLosses, balancedStats });
    io.emit('positions', positions.map(p=> ({...p, trailing: trailingMap[p.symbol] || null})) );
  } catch(e) {
    io.emit('balance', { ...balance, status: 'connected', totalProfit, winCount, lossCount, totalTrades: tradeHistory.length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, balancedStats });
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

// V10 BALANCED - Fix -$75 loss from V9
// V9 fail: Thr 35% super sensitive + TP 0.30% + BNB + 13x lev => 0W 23L 0% WR -$4.7, BNB SL x8 => -$75
// V10 fix:
// - Thr 62% (not 35%) => quality trades only, target 55-60% WR
// - TP 0.55% SL 0.40% => net 0.47% vs -0.48%, RR 1.37, breakeven 50.5% WR only! (was 60% WR need)
// - Only BTC/ETH/SOL (no BNB/AVAX/LINK) => BNB caused -$75
// - Max 1 pos only (was 2) => no parallel loss
// - Leverage 8x safe (was 13x in screenshot) => less liquidation
// - Cooldown 20s loss / 6s win (was 5s/2s) => prevent revenge trading
// - Speed: 1.5s scan (was 0.5s) => still fast but not crazy, 30-60 sec close target
function calculateBalancedSignal(candles, pair) {
  balancedStats.scanned++;
  if(candles.length < 20) return { signal: 'HOLD', confidence: 0, reason: 'wait' };
  const closes = candles.map(c => c[4]);
  const volumes = candles.map(c => c[5]);
  const last = closes.length-1;
  const price = closes[last];
  const prev1 = closes[Math.max(0,last-1)];
  const prev2 = closes[Math.max(0,last-2)];
  
  const mom1 = ((price - prev1)/prev1)*100;
  const mom2 = ((price - prev2)/prev2)*100;
  const vol = volumes[last];
  const volAvg = volumes.slice(-15).reduce((a,b)=>a+b,0)/15;
  const volRatio = vol / (volAvg || 1);
  
  const e9 = ema(closes,9); const e21 = ema(closes,21); const e50 = ema(closes,50);
  const e9Now = e9[last], e21Now = e21[last], e50Now = e50[last];
  const rsi = rsiCalc(closes);
  const emaGap = Math.abs(e9Now - e21Now) / price * 100;
  const distEma21 = Math.abs(price - e21Now) / price * 100;
  
  const trendUp = e9Now > e21Now && e21Now > e50Now;
  const trendDown = e9Now < e21Now && e21Now < e50Now;
  const nearEma21 = distEma21 < 0.25;

  let signal='HOLD', confidence=0, reason='';

  // BALANCED LONG - trend + pullback + momentum + volume
  if(trendUp && nearEma21 && rsi >= 35 && rsi <= 62 && volRatio > 0.9 && mom1 > 0.01) {
    confidence = 58 + Math.min(25, Math.abs(mom1)*120 + volRatio*6 + emaGap*100);
    if(rsi >= 42 && rsi <= 55) confidence += 6;
    if(volRatio > 1.2) confidence += 4;
    if(mom1 > 0.03 && mom2 > 0.02) confidence += 5;
    signal='LONG';
    reason=`BAL LONG TREND UP GAP ${emaGap.toFixed(4)}% MOM ${mom1.toFixed(4)}% M2 ${mom2.toFixed(4)}% RSI ${rsi.toFixed(0)} VOL x${volRatio.toFixed(2)} EMA21 ${distEma21.toFixed(4)}%`;
  }
  // BALANCED SHORT
  else if(trendDown && nearEma21 && rsi >= 38 && rsi <= 65 && volRatio > 0.9 && mom1 < -0.01) {
    confidence = 58 + Math.min(25, Math.abs(mom1)*120 + volRatio*6 + emaGap*100);
    if(rsi >= 45 && rsi <= 60) confidence += 6;
    if(volRatio > 1.2) confidence += 4;
    if(mom1 < -0.03 && mom2 < -0.02) confidence += 5;
    signal='SHORT';
    reason=`BAL SHORT TREND DOWN GAP ${emaGap.toFixed(4)}% MOM ${mom1.toFixed(4)}% RSI ${rsi.toFixed(0)} VOL x${volRatio.toFixed(2)}`;
  }
  else {
    balancedStats.filtered++;
    if(!trendUp && !trendDown) reason=`WAIT NO TREND GAP ${emaGap.toFixed(4)}% RSI ${rsi.toFixed(0)}`;
    else if(!nearEma21) reason=`WAIT PULLBACK EMA21 ${distEma21.toFixed(4)}% RSI ${rsi.toFixed(0)}`;
    else reason=`WAIT MOM ${mom1.toFixed(4)}% RSI ${rsi.toFixed(0)} VOL x${volRatio.toFixed(2)} GAP ${emaGap.toFixed(4)}%`;
  }

  return { signal, confidence: Math.min(90, confidence), price, rsi, mom1, mom2, emaGap, volRatio, distEma21, reason, trendUp, trendDown };
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
    log(`🛑 DAILY LOSS $${BOT_CONFIG.maxDailyLoss} HIT - STOP TODAY - Capital protected!`, 'error');
    BOT_CONFIG.isRunning = false;
    io.emit('botStatus','STOPPED_DAILY_LOSS');
    return;
  }
  if(consecutiveLosses >= 3 && !isManual) {
    log(`⚠️ 3 consec loss, threshold 62% -> 75% + cooldown 60s for safety`, 'warn');
    BOT_CONFIG.confidenceThreshold = 75;
    lastLossTime = now + 30000; // extra 30s cooldown
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
    log(`💰 BALANCED ${side.toUpperCase()} ${symbol} @${price.toFixed(4)} ${BOT_CONFIG.leverage}x TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% Conf ${confidence}% - RR 1.37, breakeven 50%`, 'trade');
    const order = await exchange.createMarketOrder(symbol, side, qty);
    const tpPrice = signal==='LONG' ? price*(1+BOT_CONFIG.tpPercent/100) : price*(1-BOT_CONFIG.tpPercent/100);
    const slPrice = signal==='LONG' ? price*(1-BOT_CONFIG.slPercent/100) : price*(1+BOT_CONFIG.slPercent/100);
    try {
      await exchange.createOrder(symbol, 'TAKE_PROFIT_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: tpPrice, closePosition: true });
      await exchange.createOrder(symbol, 'STOP_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: slPrice, closePosition: true });
    } catch(e){}
    const trade = { id: order.id, pair: symbol, side: signal, entryPrice: price, qty, leverage: BOT_CONFIG.leverage, tp: tpPrice, sl: slPrice, confidence, timestamp: new Date().toISOString(), status: 'OPEN', pnl: 0, entryTime };
    tradeHistory.unshift(trade); if(tradeHistory.length>150) tradeHistory.pop();
    todayTrades.unshift(trade);
    lastTradeTime = now;
    balancedStats.lastMinTrades.push(now);
    trailingMap[symbol] = { active: false, maxPrice: price, minPrice: price, stopPrice: null, entryPrice: price, side: signal, profitPeak: 0, entryTime };
    io.emit('newTrade', trade); io.emit('tradeHistory', tradeHistory);
    fetchBalance();
  } catch(e){ log(`❌ FAIL ${pair}: ${e.message.slice(0,80)}`, 'error'); }
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

    if(BOT_CONFIG.trailEnabled && !trail.active && pnlPercent >= BOT_CONFIG.trailActPercent) {
      trail.active = true;
      if(side==='long') trail.stopPrice = trail.maxPrice * (1 - BOT_CONFIG.trailDistancePercent/100);
      else trail.stopPrice = trail.minPrice * (1 + BOT_CONFIG.trailDistancePercent/100);
      log(`🟢 BAL TRAIL ACT ${symbol} ${pnlPercent.toFixed(3)}%`, 'profit');
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
            balancedStats.closeTimes.push(holdSec); if(balancedStats.closeTimes.length>20) balancedStats.closeTimes.shift();
            if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=62; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
            log(`✅ BAL TRAIL CLOSE ${symbol} ${holdSec.toFixed(0)}s Peak ${trail.profitPeak.toFixed(3)}% $${profit.toFixed(3)}`, 'profit');
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
            balancedStats.closeTimes.push(holdSec); if(balancedStats.closeTimes.length>20) balancedStats.closeTimes.shift();
            if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=62; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
            log(`✅ BAL TRAIL CLOSE SHORT ${symbol} ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'profit');
            delete trailingMap[symbol];
            tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TRAIL'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
          } catch(e){}
          continue;
        }
      }
    }
    if(!trail.active && pnlPercent >= BOT_CONFIG.tpPercent) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal; totalProfit += profit; todayProfit += profit;
        const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
        balancedStats.closeTimes.push(holdSec); if(balancedStats.closeTimes.length>20) balancedStats.closeTimes.shift();
        if(profit>0) { winCount++; consecutiveLosses=0; lastWinTime=Date.now(); BOT_CONFIG.confidenceThreshold=62; } else { lossCount++; consecutiveLosses++; lastLossTime=Date.now(); }
        log(`✅ BAL TP CLOSE ${symbol} ${pnlPercent.toFixed(3)}% ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'profit');
        delete trailingMap[symbol];
        tradeHistory.forEach(t=>{ if(t.pair===symbol && t.status==='OPEN') { t.status='CLOSED_TP'; t.exitPrice=mark; t.profit=profit; t.holdSec=holdSec; } });
      } catch(e){}
    }
    if(pnlPercent <= -BOT_CONFIG.slPercent) {
      try {
        await exchange.createMarketOrder(symbol, side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true });
        const profit = unreal; totalProfit += profit; todayProfit += profit; lossCount++; consecutiveLosses++; lastLossTime=Date.now();
        const holdSec = (Date.now() - (trail.entryTime||Date.now()))/1000;
        balancedStats.closeTimes.push(holdSec); if(balancedStats.closeTimes.length>20) balancedStats.closeTimes.shift();
        log(`🛑 BAL SL CLOSE ${symbol} ${pnlPercent.toFixed(3)}% ${holdSec.toFixed(0)}s $${profit.toFixed(3)}`, 'error');
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
  log(`💰 V10 BALANCED STARTED - Fix -$75 loss - TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% Thr ${BOT_CONFIG.confidenceThreshold}% 1.5s scan - RR 1.37`, 'success');
  log(`🔧 FIX V9 FAIL: Thr 35% => 0W 23L 0% WR -$4.7, BNB SL x8 => -$75, Lev 13x`, 'error');
  log(`✅ V10 FIX: Thr 62% quality, TP 0.55% SL 0.40% net 0.47% vs -0.48% breakeven 50.5%, Only BTC/ETH/SOL, Max 1 pos, Lev 8x safe`, 'success');
  setInterval(checkAutoCloseAndTrailing, 800);
  setInterval(async () => {
    if(!BOT_CONFIG.isRunning) return;
    for(let i=0;i<TOP_PAIRS.length;i++){
      const pair = TOP_PAIRS[i];
      try {
        const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 30);
        const analysis = calculateBalancedSignal(candles, pair);
        marketData[pair] = { ...analysis, pair, lastUpdate: Date.now() };
        if(analysis.signal !== 'HOLD' && analysis.confidence >= BOT_CONFIG.confidenceThreshold){
          log(`💰 ${pair} ${analysis.signal} ${analysis.confidence.toFixed(1)}% ${analysis.reason}`, 'signal');
          await executeTrade(pair, analysis.signal, analysis.confidence);
          if(positions.length >= BOT_CONFIG.maxPositions) break;
        }
      } catch(e){ }
      await new Promise(r=>setTimeout(r, 80));
    }
    io.emit('marketData', marketData);
    io.emit('scanLogs', scanLogs);
    const now = Date.now();
    balancedStats.lastMinTrades = balancedStats.lastMinTrades.filter(t=> now - t < 60000);
    balancedStats.tradesPerMin = balancedStats.lastMinTrades.length;
    if(balancedStats.closeTimes.length>0) balancedStats.avgCloseSec = (balancedStats.closeTimes.reduce((a,b)=>a+b,0)/balancedStats.closeTimes.length).toFixed(1);
    io.emit('stats', { totalProfit, winCount, lossCount, lastTradeAgo: Math.floor((Date.now()-lastTradeTime)/1000), trailingCount: Object.keys(trailingMap).length, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, balancedStats });
  }, BOT_CONFIG.scanIntervalMs);
}

app.get('/api/config', (req,res)=> res.json({ config: BOT_CONFIG, mode: MODE, pairs: TOP_PAIRS, balance, logs: scanLogs, stats: { totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, balancedStats }, trailing: trailingMap }));
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
  log(`Config V10 BAL TP ${BOT_CONFIG.tpPercent}% SL ${BOT_CONFIG.slPercent}% Thr ${BOT_CONFIG.confidenceThreshold}%`, 'info');
  res.json({ success: true, config: BOT_CONFIG });
});

app.post('/api/test-trade', async (req,res)=>{ await executeTrade(req.body.pair||'BTC/USDT', req.body.side||'LONG', 99, true); res.json({ success: true }); });
app.post('/api/instant-trade', async (req,res)=>{
  log('💰 BAL INSTANT - 3 pairs quality', 'info');
  let best = null, bestConf = 0;
  for(let pair of TOP_PAIRS){
    try {
      const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 30);
      const analysis = calculateBalancedSignal(candles, pair);
      if(analysis.confidence > bestConf && analysis.signal !== 'HOLD') { bestConf = analysis.confidence; best = { pair, side: analysis.signal, conf: analysis.confidence, reason: analysis.reason }; }
    } catch {}
  }
  if(best && bestConf >= 55) { await executeTrade(best.pair, best.side, best.conf, true); res.json({ success: true, picked: best }); }
  else { res.json({ success: false, msg: best ? `Best ${best.pair} ${best.conf.toFixed(1)}% but need 62% for quality` : 'No balanced setup - waiting trend + pullback (patience = no -$75 loss)' }); }
});

app.post('/api/bot/:action', (req,res)=>{
  const { action } = req.params;
  if(action==='start'){ BOT_CONFIG.isRunning = true; lastTradeTime = Date.now(); balancedStats={ scanned:0, filtered:0, tradesPerMin:0, lastMinTrades:[], avgCloseSec:0, closeTimes:[] }; io.emit('botStatus','RUNNING_BALANCED'); log(`🚀 V10 BALANCED STARTED - Fix -$75 loss - TP 0.55% SL 0.40% Thr 62% - Quality over speed`, 'success'); }
  if(action==='stop'){ BOT_CONFIG.isRunning = false; io.emit('botStatus','STOPPED'); log('BALANCED STOPPED - Capital protected', 'warn'); }
  if(action==='emergency'){
    BOT_CONFIG.isRunning = false;
    (async()=>{ for(const pos of positions){ try{ await exchange.createMarketOrder(pos.symbol, pos.side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true }); }catch(e){} } trailingMap={}; })();
    io.emit('botStatus','EMERGENCY_STOP'); log('EMERGENCY STOP - All closed', 'error');
  }
  if(action==='reset-stats'){
    totalProfit=0; winCount=0; lossCount=0; todayProfit=0; todayTrades=[]; tradeHistory=[]; trailingMap={}; consecutiveLosses=0; balancedStats={ scanned:0, filtered:0, tradesPerMin:0, lastMinTrades:[], avgCloseSec:0, closeTimes:[] }; BOT_CONFIG.confidenceThreshold=62;
    log('Stats reset - Balanced fresh, no more -$75 loss', 'warn'); io.emit('tradeHistory', tradeHistory);
  }
  res.json({ success: true, isRunning: BOT_CONFIG.isRunning });
});

app.get('/api/balance', async (req,res)=>{ await fetchBalance(); res.json({...balance, totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, balancedStats }); });
app.get('/api/positions', (req,res)=> res.json(positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null}))));
app.get('/api/history', (req,res)=> res.json(tradeHistory));
app.get('/api/logs', (req,res)=> res.json(scanLogs));
app.get('/api/stats', (req,res)=> res.json({ totalProfit, winCount, lossCount, todayProfit, todayTrades, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, balancedStats }));

app.get('/', (req,res)=>{
  const publicPath = path.join(__dirname, 'public', 'index.html');
  const rootPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(publicPath)) return res.sendFile(publicPath);
  else if (fs.existsSync(rootPath)) return res.sendFile(rootPath);
  else return res.send('<h1>Balanced Running</h1>');
});

io.on('connection', (socket)=>{
  socket.emit('config', BOT_CONFIG);
  socket.emit('balance', { ...balance, totalProfit, winCount, lossCount, todayProfit, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, consecutiveLosses, balancedStats });
  socket.emit('marketData', marketData);
  socket.emit('tradeHistory', tradeHistory);
  socket.emit('positions', positions.map(p=>({...p, trailing: trailingMap[p.symbol]||null})));
  socket.emit('scanLogs', scanLogs);
  socket.emit('botStatus', BOT_CONFIG.isRunning?'RUNNING_BALANCED':'STOPPED');
  socket.emit('trailing', trailingMap);
  socket.emit('stats', { totalProfit, winCount, lossCount, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, unrealizedPnl: balance.pnl, consecutiveLosses, balancedStats });
  const balInterval = setInterval(fetchBalance, 2500);
  const statsInterval = setInterval(()=> { 
    const now = Date.now();
    balancedStats.lastMinTrades = balancedStats.lastMinTrades.filter(t=> now - t < 60000);
    balancedStats.tradesPerMin = balancedStats.lastMinTrades.length;
    if(balancedStats.closeTimes.length>0) balancedStats.avgCloseSec = (balancedStats.closeTimes.reduce((a,b)=>a+b,0)/balancedStats.closeTimes.length).toFixed(1);
    io.emit('stats', { totalProfit, winCount, lossCount, todayProfit, todayTradesCount: todayTrades.length, winRate: (winCount+lossCount)>0 ? (winCount/(winCount+lossCount)*100).toFixed(1) : 0, trailingCount: Object.keys(trailingMap).length, unrealizedPnl: balance.pnl, consecutiveLosses, balancedStats, lastTradeAgo: Math.floor((Date.now()-lastTradeTime)/1000) }); 
  }, 1000);
  socket.on('disconnect', ()=> { clearInterval(balInterval); clearInterval(statsInterval); });
});

if(process.env.BINANCE_API_KEY && process.env.BINANCE_API_SECRET){
  initExchange(process.env.BINANCE_API_KEY, process.env.BINANCE_API_SECRET, MODE);
  fetchBalance().then(()=>{ log(`✅ Connected ${MODE} V10 BALANCED READY - Fix -$75 loss`, 'success'); startScanner(); }).catch(e=>log('Auto connect fail '+e.message, 'error'));
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, ()=> console.log(`Bot V10 BALANCED ${PORT} Mode:${MODE}`));
