const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const ccxt = require('ccxt');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));
const fs = require('fs');

const MODE = process.env.BINANCE_MODE || 'testnet';
const IS_TESTNET = MODE === 'testnet';

let BOT_CONFIG = {
  leverage: parseInt(process.env.DEFAULT_LEVERAGE) || 25,
  marginUsdt: parseFloat(process.env.DEFAULT_MARGIN_USDT) || 5,
  tpPercent: parseFloat(process.env.DEFAULT_TP_PERCENT) || 0.6,
  slPercent: parseFloat(process.env.DEFAULT_SL_PERCENT) || 0.8,
  maxPositions: parseInt(process.env.MAX_OPEN_POSITIONS) || 1,
  minUsdt: parseFloat(process.env.MIN_USDT_PER_TRADE) || 1,
  isRunning: false,
};

const TOP_PAIRS = [
  'BTC/USDT','ETH/USDT','SOL/USDT','BNB/USDT','XRP/USDT','DOGE/USDT','ADA/USDT','AVAX/USDT',
  'SHIB/USDT','DOT/USDT','LINK/USDT','TRX/USDT','MATIC/USDT','LTC/USDT','BCH/USDT','UNI/USDT',
  'ETC/USDT','XLM/USDT','ATOM/USDT','FIL/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT',
  'PEPE/USDT','WIF/USDT','BONK/USDT','FLOKI/USDT','TIA/USDT','SEI/USDT','INJ/USDT','RNDR/USDT',
  'STX/USDT','NEAR/USDT','FTM/USDT','RUNE/USDT','IMX/USDT','AAVE/USDT','MKR/USDT','GRT/USDT',
  'LDO/USDT','AGIX/USDT','FET/USDT','AR/USDT','THETA/USDT','SAND/USDT','MANA/USDT','AXS/USDT',
  'CHZ/USDT','ENJ/USDT'
];

let exchange = null;
let balance = { total: 0, free: 0, pnl: 0 };
let positions = [];
let tradeHistory = [];
let marketData = {};

function initExchange(apiKey, apiSecret, mode) {
  const isTest = mode === 'testnet';
  exchange = new ccxt.binance({
    apiKey: apiKey,
    secret: apiSecret,
    enableRateLimit: true,
    options: { defaultType: 'future', adjustForTimeDifference: true },
  });
  if(isTest) exchange.setSandboxMode(true);
  return exchange;
}

async function fetchBalance() {
  if(!exchange) return;
  try {
    const bal = await exchange.fetchBalance({ type: 'future' });
    balance.total = bal?.USDT?.total || 0;
    balance.free = bal?.USDT?.free || 0;
    const poss = await exchange.fetchPositions();
    let totalPnl = 0;
    poss.forEach(p => { if(p.unrealizedPnl) totalPnl += p.unrealizedPnl; });
    balance.pnl = totalPnl;
    positions = poss.filter(p => Math.abs(parseFloat(p.contracts || 0)) > 0);
    io.emit('balance', balance);
    io.emit('positions', positions);
  } catch(e) { console.error('Balance error:', e.message); }
}

function calculateSignal(candles) {
  if(candles.length < 200) return { signal: 'HOLD', confidence: 0 };
  const closes = candles.map(c => c[4]);
  const volumes = candles.map(c => c[5]);
  const ema = (arr, period) => {
    const k = 2/(period+1);
    let emaArr = [arr[0]];
    for(let i=1;i<arr.length;i++) emaArr.push(arr[i]*k + emaArr[i-1]*(1-k));
    return emaArr;
  };
  const rsi = (arr, period=14) => {
    let gains=0, losses=0;
    for(let i=1;i<=period;i++){
      const diff = arr[i]-arr[i-1];
      if(diff>=0) gains+=diff; else losses-=diff;
    }
    let avgGain = gains/period, avgLoss = losses/period;
    let rsis = [0];
    for(let i=period+1;i<arr.length;i++){
      const diff = arr[i]-arr[i-1];
      if(diff>=0){ avgGain = (avgGain*(period-1)+diff)/period; avgLoss = (avgLoss*(period-1))/period; }
      else { avgGain = (avgGain*(period-1))/period; avgLoss = (avgLoss*(period-1)-diff)/period; }
      const rs = avgGain/(avgLoss||0.001);
      rsis.push(100 - (100/(1+rs)));
    }
    return rsis[rsis.length-1];
  };
  const ema9 = ema(closes,9);
  const ema21 = ema(closes,21);
  const ema200 = ema(closes,200);
  const last = closes.length-1;
  const prev = last-1;
  const rsiNow = rsi(closes,14);
  const vwap = closes.slice(-20).reduce((a,b)=>a+b,0)/20;
  const volAvg = volumes.slice(-20).reduce((a,b)=>a+b,0)/20;
  const volSpike = volumes[last] > volAvg * 1.5;
  const price = closes[last];
  const ema9Now = ema9[last], ema21Now = ema21[last], ema200Now = ema200[last];
  const ema9Prev = ema9[prev], ema21Prev = ema21[prev];
  let signal='HOLD', confidence=0;
  if(price > ema200Now && price > vwap && ema9Prev <= ema21Prev && ema9Now > ema21Now && rsiNow > 40 && rsiNow < 68 && volSpike){
    signal='LONG'; confidence=85;
  } else if(price < ema200Now && price < vwap && ema9Prev >= ema21Prev && ema9Now < ema21Now && rsiNow < 60 && rsiNow > 32 && volSpike){
    signal='SHORT'; confidence=84;
  }
  const bbDist = Math.abs(price - vwap) / price;
  if(bbDist > 0.015) { signal='HOLD'; confidence=0; }
  return { signal, confidence, price, ema9: ema9Now, ema21: ema21Now, ema200: ema200Now, rsi: rsiNow, vwap, volSpike };
}

async function executeTrade(pair, signal, confidence) {
  if(!exchange || !BOT_CONFIG.isRunning) return;
  if(positions.length >= BOT_CONFIG.maxPositions) return;
  if(balance.free < BOT_CONFIG.marginUsdt) return;
  try {
    const symbol = pair;
    const amountUsdt = Math.max(BOT_CONFIG.marginUsdt, BOT_CONFIG.minUsdt);
    const ticker = await exchange.fetchTicker(symbol);
    const price = ticker.last;
    const qty = (amountUsdt * BOT_CONFIG.leverage) / price;
    try {
      await exchange.setLeverage(BOT_CONFIG.leverage, symbol);
      await exchange.setMarginMode('ISOLATED', symbol);
    } catch(e){}
    const side = signal === 'LONG' ? 'buy' : 'sell';
    const order = await exchange.createMarketOrder(symbol, side, qty);
    const tpPrice = signal==='LONG' ? price*(1+BOT_CONFIG.tpPercent/100) : price*(1-BOT_CONFIG.tpPercent/100);
    const slPrice = signal==='LONG' ? price*(1-BOT_CONFIG.slPercent/100) : price*(1-BOT_CONFIG.slPercent/100);
    try {
      await exchange.createOrder(symbol, 'TAKE_PROFIT_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: tpPrice, closePosition: true });
      await exchange.createOrder(symbol, 'STOP_MARKET', signal==='LONG'?'sell':'buy', qty, undefined, { stopPrice: slPrice, closePosition: true });
    } catch(e){}
    const trade = { id: order.id, pair: symbol, side: signal, entryPrice: price, qty, leverage: BOT_CONFIG.leverage, tp: tpPrice, sl: slPrice, confidence, timestamp: new Date().toISOString(), status: 'OPEN' };
    tradeHistory.unshift(trade);
    if(tradeHistory.length>100) tradeHistory.pop();
    io.emit('newTrade', trade);
    io.emit('tradeHistory', tradeHistory);
    fetchBalance();
  } catch(e){ io.emit('error', `Trade failed ${pair}: ${e.message}`); }
}

async function startScanner() {
  if(!exchange) return;
  setInterval(async () => {
    if(!BOT_CONFIG.isRunning) return;
    for(let i=0;i<TOP_PAIRS.length;i++){
      const pair = TOP_PAIRS[i];
      try {
        const candles = await exchange.fetchOHLCV(pair, '1m', undefined, 210);
        const analysis = calculateSignal(candles);
        marketData[pair] = { ...analysis, pair, lastUpdate: Date.now() };
        if(analysis.signal !== 'HOLD' && analysis.confidence >= 80){
          await executeTrade(pair, analysis.signal, analysis.confidence);
        }
      } catch(e){}
      await new Promise(r=>setTimeout(r, 150));
    }
    io.emit('marketData', marketData);
  }, 8000);
}

app.get('/api/config', (req,res)=> res.json({ config: BOT_CONFIG, mode: MODE, pairs: TOP_PAIRS }));
app.post('/api/config', (req,res)=>{
  const { leverage, marginUsdt, tpPercent, slPercent, maxPositions, minUsdt } = req.body;
  if(leverage) BOT_CONFIG.leverage = parseInt(leverage);
  if(marginUsdt) BOT_CONFIG.marginUsdt = parseFloat(marginUsdt);
  if(tpPercent) BOT_CONFIG.tpPercent = parseFloat(tpPercent);
  if(slPercent) BOT_CONFIG.slPercent = parseFloat(slPercent);
  if(maxPositions) BOT_CONFIG.maxPositions = parseInt(maxPositions);
  if(minUsdt) BOT_CONFIG.minUsdt = parseFloat(minUsdt);
  io.emit('config', BOT_CONFIG);
  res.json({ success: true, config: BOT_CONFIG });
});

app.post('/api/connect', async (req,res)=>{
  const { apiKey, apiSecret, mode } = req.body;
  try {
    initExchange(apiKey, apiSecret, mode||MODE);
    await exchange.fetchBalance({ type: 'future' });
    fetchBalance();
    startScanner();
    res.json({ success: true, mode: mode||MODE });
  } catch(e){ res.status(400).json({ success: false, error: e.message }); }
});

app.post('/api/bot/:action', (req,res)=>{
  const { action } = req.params;
  if(action==='start'){ BOT_CONFIG.isRunning = true; io.emit('botStatus','RUNNING'); }
  if(action==='stop'){ BOT_CONFIG.isRunning = false; io.emit('botStatus','STOPPED'); }
  if(action==='emergency'){
    BOT_CONFIG.isRunning = false;
    (async()=>{ for(const pos of positions){ try{ await exchange.createMarketOrder(pos.symbol, pos.side==='long'?'sell':'buy', Math.abs(pos.contracts), undefined, { reduceOnly: true }); }catch{} } })();
    io.emit('botStatus','EMERGENCY_STOP');
  }
  res.json({ success: true, isRunning: BOT_CONFIG.isRunning });
});

app.get('/api/balance', async (req,res)=>{ await fetchBalance(); res.json(balance); });
app.get('/api/positions', (req,res)=> res.json(positions));
app.get('/api/history', (req,res)=> res.json(tradeHistory));

// FIX: Root route - works with both public/index.html and root index.html (phone workaround)
app.get('/', (req,res)=>{
  const publicPath = path.join(__dirname, 'public', 'index.html');
  const rootPath = path.join(__dirname, 'index.html');
  const altPath = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(publicPath)) {
    return res.sendFile(publicPath);
  } else if (fs.existsSync(rootPath)) {
    return res.sendFile(rootPath);
  } else if (fs.existsSync(altPath)) {
    return res.sendFile(altPath);
  } else {
    return res.send('<h1>Bot Running but index.html not found</h1><p>GitHub eke public/index.html or index.html file eka danna</p><p>API: /api/config working? ' + JSON.stringify(BOT_CONFIG) + '</p>');
  }
});

io.on('connection', (socket)=>{
  socket.emit('config', BOT_CONFIG);
  socket.emit('balance', balance);
  socket.emit('marketData', marketData);
  socket.emit('tradeHistory', tradeHistory);
  socket.emit('positions', positions);
  const balInterval = setInterval(fetchBalance, 2000);
  socket.on('disconnect', ()=> clearInterval(balInterval));
});

if(process.env.BINANCE_API_KEY && process.env.BINANCE_API_SECRET){
  initExchange(process.env.BINANCE_API_KEY, process.env.BINANCE_API_SECRET, MODE);
  fetchBalance().then(()=>{ console.log('Auto connected from Railway env'); startScanner(); }).catch(e=>console.log('Auto connect failed', e.message));
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, ()=> console.log(`Bot running on ${PORT} Mode:${MODE}`));
