const axios = require('axios');
const Binance = require('binance-api-node').default;

// Map timeframe like '1H' -> '1h'
function normalizeInterval(tf) {
  if (!tf) return '1h';
  tf = String(tf).trim().toLowerCase();
  return tf
    .replace('h', 'h')
    .replace('m', 'm')
    .replace('d', 'd')
    .replace('w', 'w')
    .replace('mo', 'M')
    .replace('s', '');
}

function toSymbol(input) {
  if (!input) return 'BTCUSDT';
  let s = String(input).trim().toUpperCase();
  if (!s.endsWith('USDT')) s = s + 'USDT';
  return s;
}

// Simple EMA
function ema(values, period) {
  const k = 2 / (period + 1);
  let emaArr = [];
  let prev;
  for (let i = 0; i < values.length; i++) {
    const val = parseFloat(values[i]);
    if (i === 0) {
      prev = val;
    } else {
      prev = val * k + prev * (1 - k);
    }
    emaArr.push(prev);
  }
  return emaArr;
}

// SMA
function sma(values, period) {
  let res = [];
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += parseFloat(values[i]);
    if (i >= period) sum -= parseFloat(values[i - period]);
    res.push(i >= period - 1 ? sum / period : null);
  }
  return res;
}

// RSI
function rsi(closes, period = 14) {
  let gains = 0, losses = 0;
  let rsis = new Array(closes.length).fill(null);
  for (let i = 1; i < closes.length; i++) {
    const change = closes[i] - closes[i - 1];
    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);
    if (i <= period) {
      gains += gain;
      losses += loss;
      if (i === period) {
        const avgGain = gains / period;
        const avgLoss = losses / period;
        const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        rsis[i] = 100 - 100 / (1 + rs);
      }
    } else {
      // Wilder's smoothing
      gains = (gains * (period - 1) + gain) / period;
      losses = (losses * (period - 1) + loss) / period;
      const rs = losses === 0 ? 100 : gains / losses;
      rsis[i] = 100 - 100 / (1 + rs);
    }
  }
  // Forward fill first non-null
  let last = null;
  for (let i = 0; i < rsis.length; i++) {
    if (rsis[i] === null) rsis[i] = last;
    else last = rsis[i];
  }
  return rsis;
}

// MACD
function macd(closes, fast=12, slow=26, signal=9) {
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  const macdLine = emaFast.map((v, i) => v - emaSlow[i]);
  const signalLine = ema(macdLine, signal);
  const hist = macdLine.map((v, i) => v - signalLine[i]);
  return { macdLine, signalLine, hist };
}

// True Range and ATR
function atr(highs, lows, closes, period=14) {
  let trs = [];
  for (let i = 0; i < highs.length; i++) {
    const high = highs[i], low = lows[i];
    const prevClose = i > 0 ? closes[i-1] : closes[i];
    const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
    trs.push(tr);
  }
  // Wilder's smoothing
  let result = new Array(trs.length).fill(null);
  let sum = 0;
  for (let i = 0; i < trs.length; i++) {
    if (i < period) {
      sum += trs[i];
      if (i === period - 1) result[i] = sum / period;
    } else {
      result[i] = (result[i-1] * (period - 1) + trs[i]) / period;
    }
  }
  // forward fill last
  for (let i = 0; i < result.length; i++) if (result[i] == null && i>0) result[i] = result[i-1];
  return result;
}

// Stochastic %K (14)
function stochastic(highs, lows, closes, period=14) {
  let k = new Array(closes.length).fill(null);
  for (let i = 0; i < closes.length; i++) {
    const start = Math.max(0, i - period + 1);
    const hh = Math.max(...highs.slice(start, i+1));
    const ll = Math.min(...lows.slice(start, i+1));
    const c = closes[i];
    k[i] = (hh === ll) ? 50 : ((c - ll) / (hh - ll)) * 100;
  }
  return k;
}

// Momentum (ROC over 10)
function momentum(closes, period=10) {
  let m = new Array(closes.length).fill(null);
  for (let i = period; i < closes.length; i++) {
    m[i] = ((closes[i] - closes[i - period]) / closes[i - period]) * 100;
  }
  // forward fill
  for (let i = 0; i < m.length; i++) if (m[i] == null && i>0) m[i] = m[i-1];
  return m;
}

// Basic pivot points using last candle
function pivotLevels(lastHigh, lastLow, lastClose) {
  const P = (lastHigh + lastLow + lastClose) / 3;
  const R1 = 2*P - lastLow;
  const S1 = 2*P - lastHigh;
  const R2 = P + (lastHigh - lastLow);
  const S2 = P - (lastHigh - lastLow);
  const R3 = lastHigh + 2*(P - lastLow);
  const S3 = lastLow - 2*(lastHigh - P);
  return { P, R1, S1, R2, S2, R3, S3 };
}

// Fetch klines
async function fetchKlines(symbol, interval, limit=200) {
  try {
    // Use binance-api-node
    const client = Binance();
    const kl = await client.candles({ symbol, interval, limit });
    return kl.map(k => ({
      openTime: k.openTime,
      open: parseFloat(k.open),
      high: parseFloat(k.high),
      low: parseFloat(k.low),
      close: parseFloat(k.close),
      volume: parseFloat(k.volume),
      closeTime: k.closeTime
    }));
  } catch (e) {
    // Fallback to REST via axios if needed
    let url;
    if (symbol.endsWith('.P')) {
      const cleanSymbol = symbol.replace('.P', '');
      url = `https://fapi.binance.com/fapi/v1/klines?symbol=${cleanSymbol}&interval=${interval}&limit=${limit}`;
    } else {
      url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
    }
    const { data } = await axios.get(url);
    return data.map(a => ({
      openTime: a[0],
      open: parseFloat(a[1]),
      high: parseFloat(a[2]),
      low: parseFloat(a[3]),
      close: parseFloat(a[4]),
      volume: parseFloat(a[5]),
      closeTime: a[6]
    }));
  }
}

function formatNumber(n) {
  if (n === null || n === undefined || isNaN(n)) return '-';
  return Number(n).toLocaleString('tr-TR', { maximumFractionDigits: 2 });
}

// Perform analysis and return a formatted text block
async function performAnalysis(inputSymbol, inputInterval) {
  const symbol = toSymbol(inputSymbol);
  const interval = normalizeInterval(inputInterval || '1h');

  const kl = await fetchKlines(symbol, interval, 300);
  if (!kl || kl.length < 50) {
    return { text: '⚠️ Yeterli veri alınamadı.', meta: {} };
  }

  const highs = kl.map(k => k.high);
  const lows = kl.map(k => k.low);
  const closes = kl.map(k => k.close);
  const volumes = kl.map(k => k.volume);

  const last = kl[kl.length - 1];
  const price = last.close;

  // swing levels (last 50 candles)
  const lookback = 50;
  const swingHigh = Math.max(...highs.slice(-lookback));
  const swingLow = Math.min(...lows.slice(-lookback));

  // Volume dominance over last 24 candles: green vs red
  let greenVol = 0, redVol = 0;
  for (let i = kl.length - 24; i < kl.length; i++) {
    if (i <= 0) continue;
    const up = kl[i].close >= kl[i-1].close;
    if (up) greenVol += kl[i].volume;
    else redVol += kl[i].volume;
  }
  const totalVol = greenVol + redVol || 1;
  const buyersPct = (greenVol / totalVol) * 100;
  const sellersPct = 100 - buyersPct;

  // Indicators
  const rsiArr = rsi(closes, 14);
  const rsiNow = rsiArr[rsiArr.length - 1];

  const { macdLine, signalLine, hist } = macd(closes);
  const macdNow = macdLine[macdLine.length - 1];
  const macdSignal = signalLine[signalLine.length - 1];

  const atrArr = atr(highs, lows, closes, 14);
  const atrNow = atrArr[atrArr.length - 1];

  const stochK = stochastic(highs, lows, closes, 14);
  const stochNow = stochK[stochK.length - 1];

  const momArr = momentum(closes, 10);
  const momNow = momArr[momArr.length - 1];

  const ema50 = ema(closes, 50), ema200 = ema(closes, 200);
  const trendBull = ema50[ema50.length - 1] > ema200[ema200.length - 1] || (macdNow > macdSignal && rsiNow > 50);

  // Pivot levels using last candle
  const piv = pivotLevels(last.high, last.low, last.close);

  // Choose some resistance/demand levels
  const resistances = [piv.R1, piv.R2, piv.R3].filter(Boolean).sort((a,b)=>b-a);
  const supports = [piv.S1, piv.S2, piv.S3].filter(Boolean).sort((a,b)=>a-b);

  // Build text
  const lines = [];
  lines.push('🔍 Genel Değerlendirme:');
  lines.push(`- Güncel fiyat ${formatNumber(price)} civarında seyrediyor, fiyat şu an son swing hareketinin denge seviyesi üzerinde işlem görüyor.`);
  lines.push(`- Son swing hareketinin high seviyesi ~${formatNumber(swingHigh)}, low seviyesi ise ~${formatNumber(swingLow)} civarı.`);
  lines.push(`- Hacimlerde son 1 saatte alıcılar ${buyersPct.toFixed(0)}% oranında, satıcılar ${sellersPct.toFixed(0)}%.`);
  lines.push(`- Genel trend ${trendBull ? 'bullish' : 'bearish/sideways'}, ana göstergeler ${trendBull ? 'alım' : 'zayıf'} sinyali üretiyor.`);

  lines.push('\n📉 Teknik Göstergeler:\n');
  lines.push(`- MACD ${macdNow >= macdSignal ? 'bullish' : 'bearish'} (MACD: ${formatNumber(macdNow)}, Sinyal: ${formatNumber(macdSignal)})`);
  lines.push(`- RSI(14): ${formatNumber(rsiNow)} ${rsiNow >= 55 ? '(bullish)' : rsiNow <= 45 ? '(bearish)' : '(nötr)'}`);
  lines.push(`- Stochastic %K: ${formatNumber(stochNow)} ${stochNow >= 55 ? '(bullish)' : stochNow <= 45 ? '(bearish)' : '(nötr)'}`);
  lines.push(`- Momentum(10): ${formatNumber(momNow)}%`);
  lines.push(`- ATR(14): ${formatNumber(atrNow)} (volatilite ${atrNow/price > 0.005 ? 'yüksek' : 'orta/düşük'})`);

  lines.push('\n📈 Kritik Seviyeler:\n');
  if (resistances.length > 0) {
    lines.push(`- ${resistances.map(v => formatNumber(v)).join(', ')} bölgeleri fiyatın üstünde direnç/supply alanları.`);
  }
  if (supports.length > 0) {
    lines.push(`- ${supports.map(v => formatNumber(v)).join(', ')} seviyeleri fiyatın hemen altında önemli demand bölgeleri.`);
  }
  if (supports.length > 2) {
    lines.push(`- ${formatNumber(supports[0])} ise daha aşağılarda major demand seviyesi.`);
  }

  lines.push('\n🌌 Alexanın Beklentisi:\n');
  if (trendBull) {
    const pullback = supports[1] || supports[0] || piv.P;
    const targets = resistances.slice().reverse(); // ascending
    lines.push(`- Tabloya göre yükseliş olasılığı daha yüksek 🚀 Long tarafı ön planda değerlendirilebilir.`);
    lines.push(`- Fiyat ${formatNumber(pullback)} bandına geri çekilip güçlü bir dönüş sinyali verirse long işleme girilebilir. Hedefler: ${targets.map(v=>formatNumber(v)).join(', ')}.`);
    lines.push(`- Stop-loss için son swing low ve/veya ${formatNumber(supports[0] || swingLow)} altı takip edilmeli.`);
    lines.push(`- Eğer ${formatNumber(supports[0] || swingLow)} altında net kapanışlar gelirse bullish bias zayıflar; daha derin düzeltme riski oluşur.`);
    lines.push(`- İşleme girmeden önce teyit mumu/örüntüsü bekleyin; volatilite, spread ve fake-out risklerine dikkat.`);
  } else {
    const bounce = resistances[resistances.length-1] || piv.P;
    const targets = supports.slice().reverse(); // descending
    lines.push(`- Görünüm zayıf; kısa vadede satış baskısı ağır basıyor. Kısa/short tarafı daha mantıklı olabilir.`);
    lines.push(`- ${formatNumber(bounce)} civarına tepki yükselişleri satış fırsatı sunabilir. Hedefler: ${targets.map(v=>formatNumber(v)).join(', ')}.`);
    lines.push(`- Stop-loss için yakın direnç üstü takip edilmeli. ${formatNumber(resistances[0] || swingHigh)} üzeri kapanışlar senaryoyu geçersiz kılar.`);
  }

  const text = lines.join('\n');
  return {
    text,
    meta: {
      price, swingHigh, swingLow, buyersPct, rsiNow, macdNow, macdSignal, atrNow,
      resistances, supports, pivot: piv.P
    }
  };
}

module.exports = { performAnalysis };
