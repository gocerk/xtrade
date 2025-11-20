console.log('Loading binance_operations.js');

const Binance = require('binance-api-node').default;
const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');
const WebSocket = require('ws');
const { getMessage } = require('./lang');
const { buildQuery, getDecimalPlaces, getUserApiKeys, decrypt, calculateLeveragedValues, checkTradingLimits } = require('./utils');
const { getUserBinanceClient, checkIsUserSubscriber } = require('./user_management');
const { pool, dbQuery } = require('./db');
const TelegramBot = require('node-telegram-bot-api');
const bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
const orderTracker = require('./orderTracker');

console.log('Bot object:', bot ? 'Defined' : 'Undefined');
if (!bot) {
  console.error('Bot is undefined. Check bot initialization.');
  process.exit(1);
}

const POSITION_CHECK_INTERVAL = 30000;
const LOG_INTERVAL = 60000;
const CLEANUP_INTERVAL = 300000;
const MAX_RECONNECT_ATTEMPTS = 100; // Bunu güncelledik
const PING_INTERVAL = 120000;  // 2 dakika
const PONG_TIMEOUT = 45000;    // 45 saniye
const LISTEN_KEY_REFRESH = 15 * 60 * 1000; // 15 dakika
const userStreams = {};
const positionTrackers = new Map(); // "positionTrackers" olarak düzelttik (küçük p ile)
const positionLogs = new Map();
const tradeBuffer = [];
const errorBuffer = [];

const exchangeInfoCache = {
  data: null,
  lastUpdate: 0,
  TTL: 5 * 60 * 1000 // 5 dakika
};

// Her 5 dakikada bir açık pozisyonları ve order'ları kontrol et
setInterval(async () => {
  for (const [key, data] of orderTracker.activeOrders.entries()) {
      try {
          const client = await getUserBinanceClient(data.chatId);
          const orders = await client.futuresOpenOrders({ symbol: data.symbol });
          
          // Order'ların hala aktif olduğunu kontrol et
          const activeOrderIds = orders.map(o => o.orderId.toString());
          const trackedOrderIds = [data.tp1, data.tp2, data.sl].filter(Boolean);
          
          // Eksik order varsa log'la ve bildir
          const missingOrders = trackedOrderIds.filter(id => !activeOrderIds.includes(id));
          if (missingOrders.length > 0) {
              console.error(`Missing orders for ${key}:`, missingOrders);
              // Burada gerekirse yeniden oluşturma veya bildirim gönderme işlemi yapılabilir
          }
      } catch (error) {
          console.error(`Error checking orders for ${key}:`, error);
      }
  }
}, 5 * 60 * 1000);

// Exchange bilgilerini önbellekten alma fonksiyonu
async function getExchangeInfo(client) {
  const now = Date.now();
  if (!exchangeInfoCache.data || now - exchangeInfoCache.lastUpdate > exchangeInfoCache.TTL) {
      exchangeInfoCache.data = await client.futuresExchangeInfo();
      exchangeInfoCache.lastUpdate = now;
  }
  return exchangeInfoCache.data;
}

class PositionTracker {
  constructor(chatId, symbol, side) {
      this.chatId = chatId;
      this.symbol = symbol;
      this.side = side;
      this.positionId = `${symbol}_${side}_${Date.now()}`;
      this.orders = {
          entry: null,
          tp1: null,
          tp2: null,
          sl: null
      };
      this.status = 'OPENING';
      this.lastCheck = Date.now();
      this.takeProfit1 = 0;
      this.takeProfit2 = 0;
      this.stopLoss = 0;
  }

  setTakeProfitLevels(tp1, tp2, sl) {
    this.takeProfit1 = tp1;
    this.takeProfit2 = tp2;
    this.stopLoss = sl;
  }

  async initialize() {
    this.status = 'OPEN';
    return true;
  }

  async startMonitoring() {
    return true;
  }

  async destroy() {
    return true;
  }


  async logPositionStatus(isFinal = false) {
    const log = {
      timestamp: new Date().toISOString(),
      positionId: this.positionId,
      status: this.status,
      orders: this.orders,
      errors: this.errors,
      warnings: this.warnings,
      isFinal: isFinal
    };

    positionLogs.set(this.positionId, log);

    if (isFinal || this.errors.length > 0) {
      await fs.appendFile(
        'position_logs.json',
        JSON.stringify(log) + '\n'
      );
    }
  }

  async checkBeforeOpen() {
    try {
      const client = await getUserBinanceClient(this.chatId);
      const positions = await client.futuresPositionRisk();

      const existingPosition = positions.find(p =>
        p.symbol === this.symbol &&
        ((this.side === 'LONG' && parseFloat(p.positionAmt) > 0) ||
          (this.side === 'SHORT' && parseFloat(p.positionAmt) < 0))
      );

      if (existingPosition) {
        this.warnings.push({
          timestamp: new Date().toISOString(),
          type: 'EXISTING_POSITION',
          details: existingPosition
        });

        await bot.sendMessage(this.chatId,
          `⚠️ ${this.symbol} ${this.side} pozisyonunuz zaten mevcut. Yeni pozisyon açılamıyor.`);
        return false;
      }

      return true;
    } catch (error) {
      this.errors.push({
        timestamp: new Date().toISOString(),
        type: 'POSITION_CHECK_ERROR',
        error: error.message
      });
      console.error('Position check error:', error);
      return false;
    }
  }

  async validatePosition(retryCount = 3, isInitialCheck = false) {
    try {
        console.log(`[${this.symbol}] Starting validation - Status: ${this.status}, RetryCount: ${retryCount}, IsInitial: ${isInitialCheck}`);
        
        const client = await getUserBinanceClient(this.chatId);
        const position = await this.getPosition(client);
        const openOrders = await client.futuresOpenOrders({ symbol: this.symbol });

        // Sadece loglama amaçlı kontrol
        const positionInfo = {
            timestamp: new Date().toISOString(),
            symbol: this.symbol,
            chatId: this.chatId,
            status: this.status,
            hasPosition: !!position,
            positionAmount: position ? position.positionAmt : 0,
            openOrderCount: openOrders.length,
            orders: openOrders.map(o => ({
                orderId: o.orderId,
                type: o.type,
                side: o.side,
                status: o.status
            }))
        };

        // Logs klasörüne kaydet
        try {
            const logDir = path.join(__dirname, 'logs');
            await fs.mkdir(logDir, { recursive: true });
            await fs.appendFile(
                path.join(logDir, 'positions.txt'),
                JSON.stringify(positionInfo, null, 2) + '\n---\n'
            );
        } catch (logError) {
            console.error(`[${this.symbol}] Error writing position log:`, logError);
        }

        // WebSocket bağlantısını kontrol et
        const userStream = userStreams[this.chatId];
        if (!userStream || userStream.ws.readyState !== WebSocket.OPEN) {
            try {
                await stopUserStream(this.chatId);
                await startUserStream(this.chatId);
            } catch (wsError) {
                console.error(`[${this.symbol}] WebSocket reconnection failed:`, wsError);
                // Sadece loga yaz
                await fs.appendFile(
                    path.join(__dirname, 'logs', 'positions.txt'),
                    `WebSocket Error [${this.symbol}]: ${wsError.message}\n---\n`
                );
            }
        }

        // Her durumda true dön
        return true;

    } catch (error) {
        console.error(`[${this.symbol}] Validation error:`, error);
        
        // Hatayı sadece loga yaz
        try {
            const logDir = path.join(__dirname, 'logs');
            await fs.mkdir(logDir, { recursive: true });
            await fs.appendFile(
                path.join(logDir, 'positions.txt'),
                `Error [${this.symbol}]: ${error.message}\n---\n`
            );
        } catch (logError) {
            console.error(`[${this.symbol}] Error writing error log:`, logError);
        }

        // Hata durumunda bile true dön
        return true;
    }
}

async recreateOrders(client, orders, position, retryCount = 3) {
  try {
      // Önce tüm emirleri iptal et
      await client.futuresCancelAllOpenOrders({ symbol: this.symbol });
      
      // Kısa bir bekleme ekle
      await new Promise(resolve => setTimeout(resolve, 1000));

      const currentPrice = parseFloat((await client.futuresPrices({ symbol: this.symbol }))[this.symbol]);
      const positionAmt = Math.abs(parseFloat(position.positionAmt));
      const entryPrice = parseFloat(position.entryPrice);

      // Exchange bilgilerini al
      const exchangeInfo = await client.futuresExchangeInfo();
      const symbolInfo = exchangeInfo.symbols.find(s => s.symbol === this.symbol);
      const pricePrecision = symbolInfo.pricePrecision;
      const quantityPrecision = symbolInfo.quantityPrecision;

      // Price filter bilgilerini al
      const priceFilter = symbolInfo.filters.find(f => f.filterType === 'PRICE_FILTER');
      const tickSize = parseFloat(priceFilter.tickSize);

      // Yuvarlama fonksiyonları
      const roundPrice = (price) => {
          const rounded = Math.round(price / tickSize) * tickSize;
          return parseFloat(rounded.toFixed(pricePrecision));
      };
      const roundQuantity = (quantity) => parseFloat(quantity.toFixed(quantityPrecision));

      let tp1Order, tp2Order, slOrder;

      try {
          // Take Profit 1
          if (this.takeProfit1 > 0) {
              const tp1Price = roundPrice(this.side === 'LONG' ?
                  entryPrice * (1 + this.takeProfit1 / 100) :
                  entryPrice * (1 - this.takeProfit1 / 100));

              const tp1Quantity = this.takeProfit2 > 0 ?
                  roundQuantity(positionAmt / 2) :
                  roundQuantity(positionAmt);

              tp1Order = await client.futuresOrder({
                  symbol: this.symbol,
                  side: this.side === 'LONG' ? 'SELL' : 'BUY',
                  type: 'TAKE_PROFIT_MARKET',
                  timeInForce: 'GTC',
                  stopPrice: tp1Price,
                  quantity: tp1Quantity,
                  positionSide: this.side,
                  workingType: 'MARK_PRICE'
              });

              console.log(`TP1 order recreated for ${this.symbol}:`, tp1Order);
          }

          // Take Profit 2
          if (this.takeProfit2 > 0) {
              const tp2Price = roundPrice(this.side === 'LONG' ?
                  entryPrice * (1 + this.takeProfit2 / 100) :
                  entryPrice * (1 - this.takeProfit2 / 100));

              tp2Order = await client.futuresOrder({
                  symbol: this.symbol,
                  side: this.side === 'LONG' ? 'SELL' : 'BUY',
                  type: 'TAKE_PROFIT_MARKET',
                  timeInForce: 'GTC',
                  stopPrice: tp2Price,
                  quantity: roundQuantity(positionAmt / 2),
                  positionSide: this.side,
                  workingType: 'MARK_PRICE'
              });

              console.log(`TP2 order recreated for ${this.symbol}:`, tp2Order);
          }

          // Stop Loss
          if (this.stopLoss > 0) {
              const slPrice = roundPrice(this.side === 'LONG' ?
                  entryPrice * (1 - this.stopLoss / 100) :
                  entryPrice * (1 + this.stopLoss / 100));

              slOrder = await client.futuresOrder({
                  symbol: this.symbol,
                  side: this.side === 'LONG' ? 'SELL' : 'BUY',
                  type: 'STOP_MARKET',
                  timeInForce: 'GTC',
                  stopPrice: slPrice,
                  quantity: roundQuantity(positionAmt),
                  positionSide: this.side,
                  workingType: 'MARK_PRICE'
              });

              console.log(`SL order recreated for ${this.symbol}:`, slOrder);
          }

          // Emirleri tracker'a ve dosyaya kaydet
          this.orders = {
              tp1: tp1Order?.orderId || null,
              tp2: tp2Order?.orderId || null,
              sl: slOrder?.orderId || null
          };

          await saveUserOrders(this.chatId, this.symbol, {
              [`tp1_${this.side.toLowerCase()}`]: tp1Order?.orderId || null,
              [`tp2_${this.side.toLowerCase()}`]: tp2Order?.orderId || null,
              [`sl_${this.side.toLowerCase()}`]: slOrder?.orderId || null
          });

          // Emirlerin oluşturulduğunu doğrula
          const newOrders = await client.futuresOpenOrders({ symbol: this.symbol });
          if (newOrders.length === 0) {
              if (retryCount > 0) {
                  console.log(`Orders not created for ${this.symbol}, retrying... (${retryCount} attempts left)`);
                  await new Promise(resolve => setTimeout(resolve, 2000));
                  return this.recreateOrders(client, orders, position, retryCount - 1);
              } else {
                  throw new Error('Failed to recreate orders after all attempts');
              }
          }

          // Kullanıcıya bildirim gönder
          try {
              await bot.sendMessage(this.chatId,
                  `🔄 ${this.symbol} ${this.side} pozisyonu için emirler yeniden oluşturuldu:\n` +
                  `TP1: ${tp1Order ? '✅' : '❌'}\n` +
                  `TP2: ${tp2Order ? '✅' : '❌'}\n` +
                  `SL: ${slOrder ? '✅' : '❌'}`
              );
          } catch (error) {
              console.error(`Error sending order recreation notification to user ${this.chatId}:`, error);
          }

          return true;

      } catch (orderError) {
          console.error(`Error creating individual orders for ${this.symbol}:`, orderError);
          
          // Oluşturulan emirleri iptal et
          try {
              await client.futuresCancelAllOpenOrders({ symbol: this.symbol });
          } catch (cancelError) {
              console.error(`Error cancelling orders during recovery for ${this.symbol}:`, cancelError);
          }

          if (retryCount > 0) {
              console.log(`Order creation failed for ${this.symbol}, retrying... (${retryCount} attempts left)`);
              await new Promise(resolve => setTimeout(resolve, 2000));
              return this.recreateOrders(client, orders, position, retryCount - 1);
          }

          throw orderError;
      }

  } catch (error) {
      console.error(`Error in recreateOrders for ${this.symbol}:`, error);
      
      // Son deneme başarısız olduysa kullanıcıya bildir
      if (retryCount === 0) {
          try {
              await bot.sendMessage(this.chatId,
                  `⚠️ ${this.symbol} ${this.side} pozisyonu için emirler yeniden oluşturulamadı.\n` +
                  `Hata: ${error.message}\n` +
                  `Lütfen manuel olarak kontrol edin.`
              );
          } catch (notifyError) {
              console.error(`Error sending error notification to user ${this.chatId}:`, notifyError);
          }
      }

      if (retryCount > 0) {
          console.log(`Recreation failed for ${this.symbol}, retrying... (${retryCount} attempts left)`);
          await new Promise(resolve => setTimeout(resolve, 2000));
          return this.recreateOrders(client, orders, position, retryCount - 1);
      }

      throw error;
  }
}

  async validateOrder(client, orderId) {
    if (!orderId) return false;
    let retries = 3;

    while (retries > 0) {
      try {
        const order = await client.futuresGetOrder({
          symbol: this.symbol,
          orderId: orderId
        });
        return order.status === 'NEW' || order.status === 'PARTIALLY_FILLED';
      } catch (error) {
        retries--;
        this.warnings.push({
          timestamp: new Date().toISOString(),
          type: 'ORDER_VALIDATION_RETRY',
          orderId: orderId,
          retriesLeft: retries,
          error: error.message
        });

        if (retries === 0) {
          this.errors.push({
            timestamp: new Date().toISOString(),
            type: 'ORDER_VALIDATION_FAILED',
            orderId: orderId,
            error: error.message
          });
          return false;
        }

        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    return false;
  }


  async getPosition(client) {
    try {
      const positions = await client.futuresPositionRisk();
      return positions.find(p => p.symbol === this.symbol);
    } catch (error) {
      this.errors.push({
        timestamp: new Date().toISOString(),
        type: 'GET_POSITION_ERROR',
        error: error.message
      });
      return null;
    }
  }

  async handleTP1Triggered() {
    try {
        // TP1 tetiklenince hiçbir şey yapma, diğer emirler aktif kalsın
        console.log(`TP1 triggered for ${this.symbol}, keeping other orders active`);
    } catch (error) {
        console.error('TP1 handler error:', error);
    }
}

async handlePositionClose(reason) {
  try {
      const client = await getUserBinanceClient(this.chatId);
      
      if (reason === 'TP2' || reason === 'Stop Loss') {
          // Sadece TP2 veya SL tetiklendiğinde ilgili emirleri iptal et
          const orders = await getUserOrders(this.chatId, this.symbol);
          if (orders) {
              try {
                  // İlgili pozisyonun emirlerini iptal et
                  const ordersToCancel = [];
                  if (orders[`tp1_${this.side.toLowerCase()}`]) ordersToCancel.push(orders[`tp1_${this.side.toLowerCase()}`]);
                  if (orders[`tp2_${this.side.toLowerCase()}`]) ordersToCancel.push(orders[`tp2_${this.side.toLowerCase()}`]);
                  if (orders[`sl_${this.side.toLowerCase()}`]) ordersToCancel.push(orders[`sl_${this.side.toLowerCase()}`]);

                  for (const orderId of ordersToCancel) {
                      try {
                          await client.futuresCancelOrder({
                              symbol: this.symbol,
                              orderId: orderId
                          });
                      } catch (cancelError) {
                          console.error(`Error canceling order ${orderId}:`, cancelError);
                      }
                  }
              } catch (error) {
                  console.error(`Error canceling orders for ${this.symbol}:`, error);
              }
          }

          // Tracker'ı kapat
          this.status = 'CLOSED';
          positionTrackers.delete(`${this.chatId}_${this.symbol}_${this.side}`);
          
          // Emirleri dosyadan temizle
          await saveUserOrders(this.chatId, this.symbol, null);
      }
  } catch (error) {
      console.error('Position close error:', error);
    } } }

setInterval(async () => {
  const now = Date.now();
  for (const [key, tracker] of positionTrackers.entries()) {
    if (now - tracker.lastCheck > CLEANUP_INTERVAL) {
      console.log(`Cleaning up inactive tracker: ${key}`);
      await tracker.destroy();
      positionTrackers.delete(key);
    }
  }
}, CLEANUP_INTERVAL);

const TRACKER_MAX_AGE = 24 * 60 * 60 * 1000; // 24 saat

function cleanupOldTrackers() {
    const now = Date.now();
    for (const [key, tracker] of positionTrackers.entries()) {
        if (now - tracker.lastCheck > TRACKER_MAX_AGE) {
            console.log(`Removing old tracker: ${key}`);
            tracker.destroy();
            positionTrackers.delete(key);
        }
    }
}

// Her saat başı çalıştır
setInterval(cleanupOldTrackers, 60 * 60 * 1000);

function signature(queryString, apiSecret) {
  if (!apiSecret) {
    throw new Error('API Secret is undefined');
  }
  return crypto
    .createHmac('sha256', apiSecret)
    .update(queryString)
    .digest('hex');
}

async function testBinanceAPI(apiKey, apiSecret) {
  const client = Binance({
    apiKey: apiKey,
    apiSecret: apiSecret,
    futures: true
  });

  try {
    const accountInfo = await client.futuresAccountBalance();
    return Array.isArray(accountInfo);
  } catch (error) {
    console.error('Failed to validate API keys:', error.message);
    return false;
  }
}

async function saveUserOrders(userId, symbol, orders) {
  const userOrdersDir = path.join(__dirname, 'userOrders');
  const userOrdersFile = path.join(userOrdersDir, `${userId}.json`);

  try {
    await fs.mkdir(userOrdersDir, { recursive: true });
    let userOrders = {};
    try {
      const data = await fs.readFile(userOrdersFile, 'utf8');
      userOrders = JSON.parse(data);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }

    if (orders === null) {
      delete userOrders[symbol];
    } else {
      userOrders[symbol] = orders;
    }

    await fs.writeFile(userOrdersFile, JSON.stringify(userOrders, null, 2));

    await fs.appendFile(
      'orders_history.json',
      JSON.stringify({
        timestamp: new Date().toISOString(),
        userId,
        symbol,
        orders
      }) + '\n'
    );
  } catch (error) {
    console.error(`Error saving user orders for ${userId}:`, error);
  }
}

async function getUserOrders(userId, symbol) {
  const userOrdersFile = path.join(__dirname, 'userOrders', `${userId}.json`);

  try {
    const data = await fs.readFile(userOrdersFile, 'utf8');
    const userOrders = JSON.parse(data);
    return userOrders[symbol] || null;
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    console.error(`Error reading user orders for ${userId}:`, error);
    return null;
  }
}

async function executeTrade(client, chatId, tradeData) {
  const { coin, action, leverage, leveragedAmount, adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2 } = tradeData;
  const side = action.toUpperCase() === 'BUY' || action.toUpperCase() === 'LONG' ? 'LONG' : 'SHORT';
  const trackerKey = `${chatId}_${coin}_${side}`;

  console.log(`Starting trade execution for ${coin}:`, {
      side,
      leverage,
      leveragedAmount,
      adjustedStopLoss,
      adjustedTakeProfit1,
      adjustedTakeProfit2
  });

  // Mevcut pozisyon kontrolü
  if (positionTrackers.has(trackerKey)) {
      const existingTracker = positionTrackers.get(trackerKey);
      if (existingTracker.status !== 'CLOSED') {
          await bot.sendMessage(chatId, `⚠️ ${coin} ${side} pozisyonunuz zaten mevcut.`);
          return;
      }
      await existingTracker.destroy();
      positionTrackers.delete(trackerKey);
  }

  try {
    // Risk kontrolleri
    try {
        // İlk olarak tüm pozisyonları al
        const positions = await client.futuresPositionRisk();
        
        // Spesifik coin için pozisyonları filtrele
        const coinPositions = positions.filter(p => p.symbol === coin);
        
        // Her iki yöndeki pozisyonları kontrol et
        let hasLongPosition = false;
        let hasShortPosition = false;
        let longAmount = 0;
        let shortAmount = 0;

        for (const pos of coinPositions) {
            const amount = parseFloat(pos.positionAmt);
            if (Math.abs(amount) > 0.00001) { // Çok küçük (dust) pozisyonları görmezden gel
                if (amount > 0) {
                    hasLongPosition = true;
                    longAmount = amount;
                } else if (amount < 0) {
                    hasShortPosition = true;
                    shortAmount = Math.abs(amount);
                }
            }
        }

        // Aynı yönde pozisyon kontrolü
        if ((side === 'LONG' && hasLongPosition) || (side === 'SHORT' && hasShortPosition)) {
            const existingAmount = side === 'LONG' ? longAmount : shortAmount;
            await bot.sendMessage(chatId, 
                `⚠️ ${coin} için ${side} pozisyonunuz zaten mevcut. (${existingAmount} miktar)\n` +
                `Yeni işlem açılamıyor.`
            );
            return;
        }

// Ters yönde pozisyon kontrolü
if ((side === 'LONG' && hasShortPosition) || (side === 'SHORT' && hasLongPosition)) {
  const oppositeAmount = side === 'LONG' ? shortAmount : longAmount;
  
  // Kullanıcıya bilgi ver
  await bot.sendMessage(chatId,
      `🔄 ${coin} için ${side === 'LONG' ? 'SHORT' : 'LONG'} pozisyonunuz (${oppositeAmount} miktar) kapatılıyor.\n` +
      `Ardından ${side} pozisyonu açılacak.`
  );

  try {
      // 1. Tüm açık emirleri iptal et
      await client.futuresCancelAllOpenOrders({ symbol: coin });
      
      // 2. Ters pozisyonu kapat (reduceOnly parametresini kaldırdık)
      await client.futuresOrder({
          symbol: coin,
          side: side === 'LONG' ? 'BUY' : 'SELL',
          positionSide: side === 'LONG' ? 'SHORT' : 'LONG',
          type: 'MARKET',
          quantity: oppositeAmount
      });

      // 3. Pozisyonun kapandığından emin ol
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // 4. Pozisyonu tekrar kontrol et
      const verifyPositions = await client.futuresPositionRisk();
      const verifyPosition = verifyPositions.find(p => 
          p.symbol === coin && 
          ((side === 'LONG' && parseFloat(p.positionAmt) < 0) || 
          (side === 'SHORT' && parseFloat(p.positionAmt) > 0))
      );

      if (verifyPosition && Math.abs(parseFloat(verifyPosition.positionAmt)) > 0.00001) {
          throw new Error(`Eski ${side === 'LONG' ? 'SHORT' : 'LONG'} pozisyon kapatılamadı`);
      }

  } catch (closeError) {
      console.error(`Error closing opposite position for ${coin}:`, closeError);
      await bot.sendMessage(chatId,
          `❌ ${coin} ${side === 'LONG' ? 'SHORT' : 'LONG'} pozisyonu kapatılırken hata oluştu.\n` +
          `Hata: ${closeError.message}\n` +
          `Lütfen manuel olarak kontrol edin ve tekrar deneyin.`
      );
      return;
  }

  // Son bir kontrol daha yap
  const finalCheck = await client.futuresPositionRisk();
  const finalPosition = finalCheck.find(p => p.symbol === coin);
  if (finalPosition && Math.abs(parseFloat(finalPosition.positionAmt)) > 0.00001) {
      await bot.sendMessage(chatId,
          `⚠️ Eski pozisyon tam olarak kapatılamadı.\n` +
          `Lütfen pozisyonlarınızı kontrol edin.`
      );
      return;
  }
}

    } catch (error) {
        console.error(`Position check error for ${coin}:`, error);
        await bot.sendMessage(chatId,
            `❌ Pozisyon kontrolü sırasında hata oluştu.\n` +
            `Hata: ${error.message}\n` +
            `Lütfen manuel olarak kontrol edin ve tekrar deneyin.`
        );
        return;
    }

      // Position Tracker oluşturma
      const tracker = new PositionTracker(chatId, coin, side);
      tracker.setTakeProfitLevels(adjustedTakeProfit1, adjustedTakeProfit2, adjustedStopLoss);
      positionTrackers.set(trackerKey, tracker);

      // Tracker'ı OPENING durumuna getir
      tracker.status = 'OPENING';
      console.log(`Created position tracker for ${coin} ${side}`);

      // Market ve exchange bilgilerini paralel alma
      const [ticker, exchangeInfo, userSettings] = await Promise.all([
          client.futuresPrices({ symbol: coin }),
          getExchangeInfo(client),
          dbQuery('SELECT margin_type FROM user_settings WHERE user_id = ?', [chatId])
      ]);
      const currentPrice = parseFloat(ticker[coin]);
      const marginType = userSettings[0].margin_type.toUpperCase();

      // Marjin ve kaldıraç ayarlarını paralel yap
      await Promise.all([
          client.futuresMarginType({ 
              symbol: coin, 
              marginType: marginType 
          }).catch(error => {
              if (error.code !== -4046) console.warn(`Margin type error: ${error.message}`);
          }),
          client.futuresLeverage({ 
              symbol: coin, 
              leverage: leverage 
          })
      ]);

      // Sembol bilgilerini alma
      const symbolInfo = exchangeInfo.symbols.find(s => s.symbol === coin);
      const pricePrecision = symbolInfo.pricePrecision;
      const quantityPrecision = symbolInfo.quantityPrecision;

      const priceFilter = symbolInfo.filters.find(f => f.filterType === 'PRICE_FILTER');
      const tickSize = parseFloat(priceFilter.tickSize);

      // Yuvarlama fonksiyonları
      const roundPrice = (price) => {
          const rounded = Math.round(price / tickSize) * tickSize;
          return parseFloat(rounded.toFixed(pricePrecision));
      };
      const roundQuantity = (quantity) => parseFloat(quantity.toFixed(quantityPrecision));

      // Miktar hesaplama
      const quantity = leveragedAmount / currentPrice;

      const lotSizeFilter = symbolInfo.filters.find(f => f.filterType === 'LOT_SIZE');
      const stepSize = parseFloat(lotSizeFilter.stepSize);
      const minQty = parseFloat(lotSizeFilter.minQty);

      const roundedQuantity = Math.floor(quantity / stepSize) * stepSize;
      const finalQuantity = Math.max(roundedQuantity, minQty);

      console.log(`Calculated quantities for ${coin}:`, {
          rawQuantity: quantity,
          roundedQuantity,
          finalQuantity
      });

      // Giriş emri verme
      let entryOrder = await client.futuresOrder({
          symbol: coin,
          side: side === 'LONG' ? 'BUY' : 'SELL',
          type: 'MARKET',
          quantity: roundQuantity(finalQuantity),
          positionSide: side
      });

      console.log(`Entry order placed for ${coin}:`, entryOrder);

      // Giriş emrinin dolmasını bekleme
      let filledOrder;
      let attempts = 0;
      while (attempts < 10) {
          filledOrder = await client.futuresGetOrder({ symbol: coin, orderId: entryOrder.orderId });
          if (filledOrder.status === 'FILLED') {
              break;
          }
          await new Promise(resolve => setTimeout(resolve, 500));
          attempts++;
      }

      if (filledOrder.status !== 'FILLED') {
          throw new Error('Trade could not be completed.');
      }

      console.log(`Entry order filled for ${coin}:`, filledOrder);

      // 1 saniye bekle
      await new Promise(resolve => setTimeout(resolve, 500));

      // Fiyat hesaplamaları
      const entryPrice = roundPrice(parseFloat(filledOrder.avgPrice));
      const executedQty = roundQuantity(parseFloat(filledOrder.executedQty));

      let takeProfitPrice1, takeProfitPrice2;
      if (side === 'LONG') {
          takeProfitPrice1 = roundPrice(entryPrice * (1 + adjustedTakeProfit1 / 100));
          takeProfitPrice2 = adjustedTakeProfit2 > 0 ? roundPrice(entryPrice * (1 + adjustedTakeProfit2 / 100)) : 0;
      } else {
          takeProfitPrice1 = roundPrice(entryPrice * (1 - adjustedTakeProfit1 / 100));
          takeProfitPrice2 = adjustedTakeProfit2 > 0 ? roundPrice(entryPrice * (1 - adjustedTakeProfit2 / 100)) : 0;
      }

      const stopLossPrice = side === 'LONG'
          ? roundPrice(entryPrice * (1 - adjustedStopLoss / 100))
          : roundPrice(entryPrice * (1 + adjustedStopLoss / 100));

      const tp1Quantity = adjustedTakeProfit2 > 0 ? roundQuantity(executedQty / 2) : executedQty;

      // TP ve SL emirlerini paralel gönder
      const [tp1Order, tp2Order, slOrder] = await Promise.all([
          // Take Profit 1
          client.futuresOrder({
              symbol: coin,
              side: side === 'LONG' ? 'SELL' : 'BUY',
              type: 'TAKE_PROFIT_MARKET',
              timeInForce: 'GTC',
              stopPrice: takeProfitPrice1,
              quantity: tp1Quantity,
              positionSide: side,
              workingType: 'MARK_PRICE'
          }),
          // Take Profit 2 (eğer varsa)
          adjustedTakeProfit2 > 0 ? client.futuresOrder({
              symbol: coin,
              side: side === 'LONG' ? 'SELL' : 'BUY',
              type: 'TAKE_PROFIT_MARKET',
              timeInForce: 'GTC',
              stopPrice: takeProfitPrice2,
              quantity: roundQuantity(executedQty / 2),
              positionSide: side,
              workingType: 'MARK_PRICE'
          }) : Promise.resolve(null),
          // Stop Loss
          client.futuresOrder({
              symbol: coin,
              side: side === 'LONG' ? 'SELL' : 'BUY',
              type: 'STOP_MARKET',
              timeInForce: 'GTC',
              stopPrice: stopLossPrice,
              quantity: roundQuantity(executedQty),
              positionSide: side,
              workingType: 'MARK_PRICE'
          })
      ]);

      console.log(`All orders placed for ${coin}`);


    await orderTracker.saveOrders(chatId, coin, side, {
      tp1: tp1Order.orderId,
      tp2: tp2Order ? tp2Order.orderId : null,
      sl: slOrder.orderId
    });

      // Emirleri tracker'a kaydetme
      tracker.orders = {
          entry: entryOrder.orderId,
          tp1: tp1Order.orderId,
          tp2: tp2Order ? tp2Order.orderId : null,
          sl: slOrder.orderId
      };

      // Emirleri dosyaya kaydetme
      await saveUserOrders(chatId, coin, {
          [`tp1_${side.toLowerCase()}`]: tp1Order.orderId,
          [`tp2_${side.toLowerCase()}`]: tp2Order ? tp2Order.orderId : null,
          [`sl_${side.toLowerCase()}`]: slOrder.orderId
      });

      // Pozisyonu doğrula ve initialize et
      await new Promise(resolve => setTimeout(resolve, 1000));

      try {
          // İlk pozisyon ve emir kontrolü
          const [openOrders, position] = await Promise.all([
              client.futuresOpenOrders({ symbol: coin }),
              client.futuresPositionRisk({ symbol: coin })
          ]);
          
          const activePosition = position.find(p => p.symbol === coin && parseFloat(p.positionAmt) !== 0);

          if (!activePosition) {
              throw new Error('Position not found after creation');
          }

          if (openOrders.length < 2) {
              console.log('Orders missing, recreating...');
              await tracker.recreateOrders(client, {
                  [`tp1_${side.toLowerCase()}`]: tp1Order.orderId,
                  [`tp2_${side.toLowerCase()}`]: tp2Order ? tp2Order.orderId : null,
                  [`sl_${side.toLowerCase()}`]: slOrder.orderId
              }, activePosition);
          }

          // Tracker'ı başlat
          console.log(`[${coin}] Trade execution completed, setting up monitoring...`);

          // Önce tracker'ı başlat
          await tracker.initialize();
          const monitoringResult = await tracker.startMonitoring();

          if (!monitoringResult) {
              console.error(`[${coin}] Failed to start position monitoring`);
              await bot.sendMessage(chatId,
                  `⚠️ ${coin} ${side} pozisyonu açıldı fakat izleme başlatılamadı.\n` +
                  `Lütfen pozisyonunuzu manuel olarak takip edin.`
              );
          }

          // İşlem mesajını oluştur
          const tradeMessage = getMessage('tradeOpened', {
              action: action.toUpperCase(),
              coin: coin,
              executedQty: executedQty,
              entryPrice: entryPrice,
              actualCost: (entryPrice * executedQty).toFixed(2),
              takeProfitPrice1: takeProfitPrice1,
              takeProfitPrice2: takeProfitPrice2 > 0 ? takeProfitPrice2 : 'N/A',
              stopLossPrice: stopLossPrice,
              leverage: leverage,
              marginType: marginType
          });

          // İşlemi kaydet
          await saveTrade(chatId, coin, executedQty, action, leverage, leveragedAmount,
              adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2, marginType);

          console.log(`[${coin}] Trade setup completed successfully`);
          return tradeMessage;

      } catch (error) {
          console.error(`Error in trade finalization for ${coin}:`, error);
          await fs.appendFile(
              path.join(__dirname, 'logs', 'trade_errors.txt'),
              `${new Date().toISOString()} - Error in ${coin}: ${error.message}\n`
          );
          return getMessage('tradeOpened', {
              action: action.toUpperCase(),
              coin: coin,
              executedQty: executedQty,
              entryPrice: entryPrice,
              actualCost: (entryPrice * executedQty).toFixed(2),
              takeProfitPrice1: takeProfitPrice1,
              takeProfitPrice2: takeProfitPrice2 > 0 ? takeProfitPrice2 : 'N/A',
              stopLossPrice: stopLossPrice,
              leverage: leverage,
              marginType: marginType
          });
      }

  } catch (error) {
      console.error(`Failed to open trade for User ${chatId}:`, error);
      positionTrackers.delete(trackerKey);
      const errorMessage = getMessage('tradeExecutionError', { error: error.message });
      try {
          await bot.sendMessage(chatId, errorMessage);
      } catch (sendError) {
          console.error(`Failed to send error message to user ${chatId}:`, sendError);
      }
      await saveError(chatId, 'Trade execution failed', error.message);
      throw error;
  }
}

async function autoTradeUsers(coin, action, price) {
  try {
    const users = await dbQuery('SELECT * FROM user_settings WHERE autotrade = 1');

    for (const user of users) {
      const userId = user.user_id;

      try {
        const subsinfo = await checkIsUserSubscriber(userId);
        if (!subsinfo.isSubscriber) {
          console.log(`User ${userId} will not be auto-traded due to lack of subscription.`);
          continue;
        }

        const client = await getUserBinanceClient(userId);
        const symbol = coin.replace('/', '').toUpperCase();
        const { amount_per_trade, leverage, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type } = user;

        const { leveragedAmount, adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2 } = calculateLeveragedValues(
          amount_per_trade,
          leverage,
          stop_loss_percent,
          profit_percent_1,
          profit_percent_2
        );

        const tradeData = {
          coin: symbol,
          action,
          leverage,
          leveragedAmount,
          adjustedStopLoss,
          adjustedTakeProfit1,
          adjustedTakeProfit2,
          marginType: margin_type || 'ISOLATED'
        };

        const tradeMessage = await executeTrade(client, userId, tradeData);

        // Trade başarılı olduğunda mesaj gönder
        if (tradeMessage) {  // Sadece tradeMessage varsa mesaj gönder
          try {
            await bot.sendMessage(userId, getMessage('autoTradeExecuted', {
              coin: symbol,
              action: action,
              amount: amount_per_trade,
              leverage: leverage,
              profitTarget1: profit_percent_1,
              profitTarget2: profit_percent_2,
              stopLoss: stop_loss_percent,
              marginType: margin_type || 'ISOLATED'
            }));
          } catch (sendError) {
            console.error(`Failed to send auto-trade message to user ${userId}:`, sendError);
          }
        }

      } catch (error) {
        console.error(`Failed to execute auto-trade for User ${userId}:`, error);
        try {
          await bot.sendMessage(userId, getMessage('autoTradeError', { error: error.message }));
        } catch (sendError) {
          console.error(`Failed to send error message to user ${userId}:`, sendError);
        }
        await saveError(userId, 'Auto-trade execution failed', error.message);
      }
    }
  } catch (err) {
    console.error('Error in autoTradeUsers:', err);
  }
}

async function saveTrade(userId, symbol, amount, action, leverage, leveragedAmount, adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2, marginType) {
  const trade = {
    userId,
    symbol,
    amount,
    action,
    leverage,
    leveragedAmount,
    adjustedStopLoss,
    adjustedTakeProfit1,
    adjustedTakeProfit2,
    marginType,
    timestamp: new Date().toISOString()
  };

  tradeBuffer.push(trade);
  if (tradeBuffer.length >= 10) {
    try {
      await fs.appendFile('trades.json', tradeBuffer.map(t => JSON.stringify(t)).join('\n') + '\n');
      tradeBuffer.length = 0;
    } catch (err) {
      console.error('Error writing trades to file:', err);
    }
  }
}

// Ve en alta buffer temizleme interval'ını ekle
setInterval(async () => {
  if (tradeBuffer.length > 0) {
    try {
      await fs.appendFile('trades.json', tradeBuffer.map(t => JSON.stringify(t)).join('\n') + '\n');
      tradeBuffer.length = 0;
    } catch (err) {
      console.error('Error writing trades to file:', err);
    }
  }
  if (errorBuffer.length > 0) {
    try {
      await fs.appendFile('errors.json', errorBuffer.map(e => JSON.stringify(e)).join('\n') + '\n');
      errorBuffer.length = 0;
    } catch (err) {
      console.error('Error writing errors to file:', err);
    }
  }
}, 5 * 60 * 1000);

async function saveError(userId, errorType, errorMessage) {
  const error = {
    userId,
    errorType,
    errorMessage,
    timestamp: new Date().toISOString()
  };

  errorBuffer.push(error);
  if (errorBuffer.length >= 5) {
    try {
      await fs.appendFile('errors.json', errorBuffer.map(e => JSON.stringify(e)).join('\n') + '\n');
      errorBuffer.length = 0;
    } catch (err) {
      console.error('Error writing errors to file:', err);
    }
  }
}

async function startUserStream(chatId) {
  try {
    const userApiKeys = await getUserApiKeys(chatId);
    if (!userApiKeys) {
      throw new Error('API anahtarları bulunamadı');
    }

    const client = Binance({
      apiKey: userApiKeys.apiKey,
      apiSecret: userApiKeys.apiSecret,
      futures: true
    });

    const listenKeyResponse = await client.futuresGetDataStream();
    const listenKey = listenKeyResponse.listenKey;
    console.log('Listen key:', listenKey);

    const ws = new WebSocket(`wss://fstream.binance.com/ws/${listenKey}`);

    ws.on('open', () => {
      console.log(`WebSocket bağlantısı açıldı: ${chatId}`);
    });

    ws.on('close', () => {
      console.log(`WebSocket bağlantısı kapandı: ${chatId}`);
      setTimeout(() => startUserStream(chatId), 5000); // 5 saniye sonra yeniden bağlan
    });

    ws.on('message', async (data) => {
      try {
          const parsedData = JSON.parse(data.toString());
          
          if (parsedData.e === 'ORDER_TRADE_UPDATE' && parsedData.o.X === 'FILLED') {
              const order = parsedData.o;
              const orderId = order.i;
  
              const orderInfo = orderTracker.getOrderInfo(orderId);
              if (!orderInfo) {
                  console.log(`Order ${orderId} not tracked, ignoring`);
                  return;
              }
  
              // orderInfo'dan chatId'yi alalım
              const { chatId } = orderInfo;
  
              console.log(`Processing order ${orderId}:`, orderInfo);
  
              const symbol = order.s;
              const side = order.ps;
              const realizedPnl = parseFloat(order.rp || 0);
              
              // Pozisyon ve fiyat bilgilerini al
              const position = await client.futuresPositionRisk({ symbol });
              const positionData = position.find(p => p.symbol === symbol);
              const leverage = parseInt(positionData?.leverage || 1);
  
              // Fiyat bilgilerini al
              const executionPrice = parseFloat(order.L || order.ap);
              let entryPrice = parseFloat(order.b || order.ap);
  
              // Eğer hala 0 ise başka bir yöntem
              if (!entryPrice || entryPrice === 0) {
                  // Order datası içinden entry price'ı almaya çalış
                  entryPrice = parseFloat(order.sp || order.ep || positionData?.entryPrice);
              }
  
              // Debug için
              console.log('Price Data:', {
                  rawOrderData: order,
                  executionPrice,
                  entryPrice,
                  positionEntryPrice: positionData?.entryPrice
              });
  
              // ROE hesaplama
              let roe = 0;
              if (entryPrice && executionPrice) {
                  if (side === 'LONG') {
                      roe = ((executionPrice - entryPrice) / entryPrice) * 100 * leverage;
                  } else {
                      roe = ((entryPrice - executionPrice) / entryPrice) * 100 * leverage;
                  }
              }
  
              let emoji = '🎯';
              let actionEmoji = side === 'LONG' ? '📈' : '📉';
              let profitEmoji = realizedPnl > 0 ? '💰' : '❌';
              
              if (orderInfo.type.includes('TP')) {
                  console.log('Processing TP order:', {
                      orderInfo,
                      orderId,
                      type: orderInfo.type,
                      chatId
                  });
  
                  const message = 
                      `${emoji} TAKE PROFIT ${actionEmoji}\n\n` +
                      `${symbol} ${side} Pozisyonu ${orderInfo.type === 'TP2' ? 'Tamamen ' : 'İlk Yarısı '} Kapandı!\n\n` +
                      `${profitEmoji} Kar/Zarar: ${realizedPnl.toFixed(2)} USDT\n` +
                      `📌 Giriş Fiyatı: ${entryPrice.toFixed(8)}\n` +
                      `🎯 Çıkış Fiyatı: ${executionPrice.toFixed(8)}\n` +
                      `🔋 Kaldıraç: ${leverage}x`;
  
                  try {
                      if (!chatId) {
                          console.error('ChatId is missing:', orderInfo);
                          return;
                      }
  
                      await bot.sendMessage(chatId, message);
                      console.log(`Message sent for ${orderInfo.type}`, { chatId, symbol, side });
                      
                      if (orderInfo.type === 'TP2') {
                          await orderTracker.removeOrders(chatId, symbol, side);
                          console.log('Orders removed after TP2');
                      }
                  } catch (error) {
                      console.error('Error sending TP message:', error, {
                          chatId,
                          orderInfo
                      });
                  }
              } else if (orderInfo.type === 'SL') {
                  if (!chatId) {
                      console.error('ChatId is missing for SL:', orderInfo);
                      return;
                  }
  
                  const message = 
                      `🛑 STOP LOSS ${actionEmoji}\n\n` +
                      `${symbol} ${side} Pozisyonu Kapandı!\n\n` +
                      `❌ Kar/Zarar: ${realizedPnl.toFixed(2)} USDT\n` +
                      `📌 Giriş Fiyatı: ${entryPrice.toFixed(8)}\n` +
                      `🔻 Çıkış Fiyatı: ${executionPrice.toFixed(8)}\n` +
                      `🔋 Kaldıraç: ${leverage}x`;
  
                  try {
                      await bot.sendMessage(chatId, message);
                      await orderTracker.removeOrders(chatId, symbol, side);
                  } catch (error) {
                      console.error('Error sending SL message:', error, {
                          chatId,
                          orderInfo
                      });
                  }
              }
  
              console.log('Order Execution Details:', {
                  symbol,
                  side,
                  orderType: orderInfo.type,
                  entryPrice,
                  executionPrice,
                  realizedPnl,
                  roe,
                  leverage,
                  chatId
              });
  
              await fs.appendFile(
                  path.join(__dirname, 'logs', 'order_executions.log'),
                  `${new Date().toISOString()} - ${orderInfo.type} executed for ${symbol} ${side} (Order ID: ${orderId}) - PNL: ${realizedPnl} - ROE: ${roe}% - Entry: ${entryPrice} - Exit: ${executionPrice}\n`
              );
          }
      } catch (error) {
          console.error(`Error processing message:`, error);
      }
  });

    ws.on('error', (error) => {
      console.error(`WebSocket hatası (${chatId}):`, error);
    });

    // Listen Key yenileme - 30 dakikada bir
    const refreshInterval = setInterval(async () => {
      try {
        await client.futuresKeepDataStream({ listenKey: listenKey });
        console.log(`Listen key yenilendi (chatId: ${chatId})`);
      } catch (error) {
        console.error(`Listen key yenilenirken hata oluştu (chatId: ${chatId}):`, error);
      }
    }, 30 * 60 * 1000);

    // Ping/Pong - 3 dakikada bir
    const pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
        console.log(`Ping sent (chatId: ${chatId})`);
      }
    }, 3 * 60 * 1000);

    ws.on('pong', () => {
      console.log(`Pong received (chatId: ${chatId})`);
    });

    // 6 saatte bir yeniden başlat
    const restartInterval = setInterval(async () => {
      try {
        console.log(`Restarting stream for user ${chatId}`);
        await stopUserStream(chatId);
        await startUserStream(chatId);
        console.log(`Successfully restarted stream for user ${chatId}`);
      } catch (error) {
        console.error(`Error restarting stream for user ${chatId}:`, error);
      }
    }, 6 * 60 * 60 * 1000);

    await updateUserNotificationStatus(chatId, true);

    userStreams[chatId] = { ws, refreshInterval, pingInterval, restartInterval, listenKey };

    console.log(`User stream started for chatId: ${chatId}`);
    return true;
  } catch (error) {
    console.error(`User stream başlatılırken hata oluştu (chatId: ${chatId}):`, error);
    await bot.sendMessage(chatId, 'Stop loss ve take profit bildirimlerini başlatırken bir hata oluştu. Lütfen daha sonra tekrar deneyin.');
    return false;
  }
}

async function stopUserStream(chatId) {
  try {
    const userStream = userStreams[chatId];
    if (userStream) {
      if (userStream.ws) {
        userStream.ws.close();
      }
      if (userStream.refreshInterval) {
        clearInterval(userStream.refreshInterval);
      }
      if (userStream.pingInterval) {
        clearInterval(userStream.pingInterval);
      }
      if (userStream.restartInterval) {
        clearInterval(userStream.restartInterval);
      }
      if (userStream.healthCheckInterval) {
        clearInterval(userStream.healthCheckInterval);
      }

      const userApiKeys = await getUserApiKeys(chatId);
      if (userApiKeys) {
        const client = Binance({
          apiKey: userApiKeys.apiKey,
          apiSecret: userApiKeys.apiSecret,
          futures: true
        });
        await client.futuresCloseDataStream({ listenKey: userStream.listenKey });
      }

      delete userStreams[chatId];
    }

    await updateUserNotificationStatus(chatId, false);

    for (const [key, tracker] of positionTrackers.entries()) {
      if (tracker.chatId === chatId) {
        await tracker.destroy();
        positionTrackers.delete(key);
      }
    }

    return true;
  } catch (error) {
    console.error(`User stream durdurulurken hata oluştu (chatId: ${chatId}):`, error);
    await bot.sendMessage(chatId, 'Bildirimleri durdururken bir hata oluştu. Lütfen daha sonra tekrar deneyin.');
    return false;
  }
}

async function getUserNotificationStatus(chatId) {
  try {
    const results = await dbQuery('SELECT notifications FROM user_settings WHERE user_id = ?', [chatId]);
    return results.length > 0 ? results[0].notifications === 1 : false;
  } catch (error) {
    console.error('Bildirim durumu alınırken hata oluştu:', error);
    throw error;
  }
}

async function updateUserNotificationStatus(chatId, status) {
  try {
    await dbQuery('INSERT INTO user_settings (user_id, notifications) VALUES (?, ?) ON DUPLICATE KEY UPDATE notifications = ?',
      [chatId, status ? 1 : 0, status ? 1 : 0]);
  } catch (error) {
    console.error('Bildirim durumu güncellenirken hata oluştu:', error);
    throw error;
  }
}

async function restartAllUserStreams() {
  try {
    const rows = await dbQuery('SELECT user_id FROM user_settings WHERE notifications = 1');

    for (const row of rows) {
      const chatId = row.user_id;
      try {
        await startUserStream(chatId);
      } catch (err) {
        console.error(`Error restarting stream for user ${chatId}:`, err);
      }
    }
  } catch (error) {
    console.error('Error querying user settings:', error);
    throw error;
  }
}

async function cancelAllOpenOrders(chatId, symbol) {
  try {
    const client = await getUserBinanceClient(chatId);
    if (!client) return false;

    await client.futuresCancelAllOpenOrders({ symbol });
    await saveUserOrders(chatId, symbol, null);

    return true;
  } catch (error) {
    console.error(`Error cancelling orders for ${chatId}, ${symbol}:`, error);
    return false;
  }
}

async function getPositionSize(client, symbol) {
  try {
    const positions = await client.futuresPositionRisk();
    const position = positions.find(p => p.symbol === symbol);
    return position ? Math.abs(parseFloat(position.positionAmt)) : 0;
  } catch (error) {
    console.error(`Error getting position size for ${symbol}:`, error);
    return 0;
  }
}

async function closePosition(chatId, symbol, side) {
  try {
    const client = await getUserBinanceClient(chatId);
    if (!client) return false;

    const positions = await client.futuresPositionRisk();
    const position = positions.find(p => p.symbol === symbol);

    if (position && parseFloat(position.positionAmt) !== 0) {
      await client.futuresOrder({
        symbol: symbol,
        side: side === 'LONG' ? 'SELL' : 'BUY',
        type: 'MARKET',
        quantity: Math.abs(parseFloat(position.positionAmt)),
        positionSide: side
      });

      await cancelAllOpenOrders(chatId, symbol);
      return true;
    }

    return false;
  } catch (error) {
    console.error(`Error closing position for ${chatId}, ${symbol}:`, error);
    return false;
  }
}

async function closeAllPositionsAndOrders(client, symbol) {
  try {
    await client.futuresCancelAllOpenOrders({ symbol });

    const positions = await client.futuresPositionRisk({ symbol });
    const position = positions.find(p => p.symbol === symbol);

    if (position && parseFloat(position.positionAmt) !== 0) {
      const side = parseFloat(position.positionAmt) > 0 ? 'SELL' : 'BUY';
      const quantity = Math.abs(parseFloat(position.positionAmt));

      await client.futuresOrder({
        symbol: symbol,
        side: side,
        type: 'MARKET',
        quantity: quantity
      });

      return true;
    }

    return false;
  } catch (error) {
    console.error(`Error in closeAllPositionsAndOrders for ${symbol}:`, error);
    return false;
  }
}

async function cancelRelatedOrders(chatId, symbol, triggeredOrderId) {
  try {
    const userOrders = await getUserOrders(chatId, symbol);
    if (!userOrders) return;

    const client = await getUserBinanceClient(chatId);
    if (!client) return;

    const orderIds = Object.values(userOrders).filter(id => id && id !== triggeredOrderId);

    for (const orderId of orderIds) {
      try {
        await client.futuresCancelOrder({
          symbol: symbol,
          orderId: orderId
        });
      } catch (error) {
        if (error.code !== -2011) { // Order does not exist
          console.error(`Failed to cancel order ${orderId} for ${symbol}:`, error);
        }
      }
    }

    await saveUserOrders(chatId, symbol, null);
  } catch (error) {
    console.error(`Error in cancelRelatedOrders for ${chatId}, ${symbol}:`, error);
  }
}

module.exports = {
  testBinanceAPI,
  executeTrade,
  autoTradeUsers,
  startUserStream,
  stopUserStream,
  getUserNotificationStatus,
  updateUserNotificationStatus,
  restartAllUserStreams,
  cancelAllOpenOrders,
  closePosition,
  closeAllPositionsAndOrders,
  getPositionSize,
  cancelRelatedOrders,
  positionTrackers
};