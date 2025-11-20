const fs = require('fs').promises;
const path = require('path');
const Binance = require('binance-api-node').default;
const TelegramBot = require('node-telegram-bot-api');
const bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
const { dbQuery, encryptionKey } = require('./db');
const crypto = require('crypto');

function decrypt(encryptedText) {
    try {
        if (!encryptedText) return null;
        const textParts = encryptedText.split(':');
        const iv = Buffer.from(textParts.shift(), 'hex');
        const encryptedData = Buffer.from(textParts.join(':'), 'hex');
        const decipher = crypto.createDecipheriv('aes-256-cbc', encryptionKey, iv);
        let decrypted = decipher.update(encryptedData);
        decrypted = Buffer.concat([decrypted, decipher.final()]);
        return decrypted.toString();
    } catch (error) {
        console.error('Decrypt error:', error);
        return null;
    }
}

async function checkPositionExists(client, symbol, side) {
    try {
        const positions = await client.futuresPositionRisk({ symbol });
        
        for (const position of positions) {
            if (position.symbol !== symbol) continue;
            
            const positionAmt = parseFloat(position.positionAmt);
            const positionSide = position.positionSide;

            if (side === 'LONG' && positionSide === 'LONG' && positionAmt > 0) {
                console.log(`[OrderTracker] ${symbol} LONG pozisyon bulundu: ${positionAmt}`);
                return {exists: true, amount: positionAmt, position};
            }
            if (side === 'SHORT' && positionSide === 'SHORT' && positionAmt < 0) {
                console.log(`[OrderTracker] ${symbol} SHORT pozisyon bulundu: ${positionAmt}`);
                return {exists: true, amount: Math.abs(positionAmt), position};
            }
        }

        console.log(`[OrderTracker] ${symbol} ${side} pozisyon bulunamadı`);
        return {exists: false, amount: 0, position: null};
    } catch (error) {
        console.error(`[OrderTracker] Pozisyon kontrol hatası:`, error);
        return {exists: false, amount: 0, position: null};
    }
}

async function closePosition(client, symbol, side, amount) {
    try {
        const orderParams = {
            symbol: symbol,
            type: 'MARKET',
            quantity: amount,
            positionSide: side,
            side: side === 'LONG' ? 'SELL' : 'BUY'
        };

        await client.futuresOrder(orderParams);
        console.log(`[OrderTracker] ${symbol} ${side} pozisyonu kapatıldı. Miktar: ${amount}`);
        return true;
    } catch (error) {
        console.error(`[OrderTracker] Pozisyon kapatma hatası (${side}):`, error);
        return false;
    }
}

class OrderTracker {
    constructor() {
        this.dataDir = path.join(__dirname, 'data');
        this.orderStorageFile = path.join(this.dataDir, 'activeOrders.json');
        this.activeOrders = new Map();
        this.init();
        
        setInterval(async () => {
            console.log('[OrderTracker] Periyodik kontrol başlatılıyor...');
            await this.validateAllOrders();
        }, 60 * 1000); // 1 dakika

        process.on('SIGINT', async () => {
            console.log('[OrderTracker] Bot kapanıyor, son durum kaydediliyor...');
            await this.persistToDisk();
            process.exit();
        });
    }

    async init() {
        try {
            await fs.mkdir(this.dataDir, { recursive: true });
            try {
                const data = await fs.readFile(this.orderStorageFile, 'utf8');
                const orders = JSON.parse(data);
                for (const [key, value] of Object.entries(orders)) {
                    this.activeOrders.set(key, value);
                }
                console.log('[OrderTracker] Dosyadan yüklendi');
                await this.validateAllOrders();
            } catch (error) {
                if (error.code !== 'ENOENT') {
                    console.error('[OrderTracker] Veri yükleme hatası:', error);
                } else {
                    await fs.writeFile(this.orderStorageFile, JSON.stringify({}));
                    console.log('[OrderTracker] Yeni depolama dosyası oluşturuldu');
                }
            }
        } catch (error) {
            console.error('[OrderTracker] Başlatma hatası:', error);
        }
    }

    async validateAllOrders() {
        console.log('[OrderTracker] Tüm aktif emirler kontrol ediliyor...');
        const ordersToRemove = [];
    
        try {
            const users = await dbQuery(`
                SELECT DISTINCT ua.user_id as chat_id, ua.api_key, ua.api_secret
                FROM user_api_keys ua
                INNER JOIN user_settings us ON ua.user_id = us.user_id
                WHERE us.autotrade = 1
                AND ua.api_key IS NOT NULL 
                AND ua.api_secret IS NOT NULL
            `);
            
            console.log(`[OrderTracker] ${users.length} aktif kullanıcı için kontrol yapılıyor`);
    
            for (const user of users) {
                try {
                    const decryptedApiKey = decrypt(user.api_key);
                    const decryptedApiSecret = decrypt(user.api_secret);
    
                    if (!decryptedApiKey || !decryptedApiSecret) {
                        console.error(`[OrderTracker] API key şifre çözme hatası: ${user.chat_id}`);
                        continue;
                    }
    
                    const client = Binance({
                        apiKey: decryptedApiKey,
                        apiSecret: decryptedApiSecret,
                        futures: true
                    });
    
                    const allPositions = await client.futuresPositionRisk();
                    const activePositions = allPositions.filter(p => {
                        const amt = parseFloat(p.positionAmt);
                        return (p.positionSide === 'LONG' && amt > 0) || (p.positionSide === 'SHORT' && amt < 0);
                    });
    
                    const allOpenOrders = await client.futuresOpenOrders();
                    console.log(`[OrderTracker] ${user.chat_id} kullanıcısının açık emirleri:`, allOpenOrders.length);
                    
                    // Önce tüm pozisyonları ve emirleri bir cache'e alalım
                    const positionCache = new Map();
                    const orderCache = new Map();
                    
                    activePositions.forEach(pos => {
                        const key = `${pos.symbol}_${pos.positionSide}`;
                        positionCache.set(key, pos);
                    });
                    
                    allOpenOrders.forEach(order => {
                        const key = `${order.symbol}_${order.positionSide}`;
                        if (!orderCache.has(key)) {
                            orderCache.set(key, []);
                        }
                        orderCache.get(key).push(order);
                    });
    
// Emirsiz pozisyonları kontrol et kısmında:
for (const [key, position] of positionCache) {
    const orders = orderCache.get(key) || [];
    const positionAmt = Math.abs(parseFloat(position.positionAmt));
    
    if (positionAmt > 0) {
        // Pozisyon yaşını kontrol et (milisaniye cinsinden)
        const positionAge = Date.now() - position.updateTime;
        const MIN_POSITION_AGE = 20000; // 20 saniye

        // Sadece 20 saniye ve üzeri yaştaki pozisyonları kontrol et
        if (positionAge >= MIN_POSITION_AGE) {
            // Stop Loss ve Take Profit emirlerini kontrol et
            const hasStopLoss = orders.some(order => 
                order.type === 'STOP_MARKET' || 
                order.type === 'STOP' || 
                order.type === 'STOP_LOSS_LIMIT' ||
                order.type === 'STOP_LOSS'
            );
            
            const hasTakeProfit = orders.some(order => 
                order.type === 'TAKE_PROFIT_MARKET' || 
                order.type === 'TAKE_PROFIT' || 
                order.type === 'LIMIT'
            );

            // Eğer SL veya TP'den biri eksikse pozisyonu kapat
            if (!hasStopLoss || !hasTakeProfit) {
                console.log(`[OrderTracker] ${position.symbol} ${position.positionSide} pozisyonu için ${!hasStopLoss ? 'Stop Loss' : 'Take Profit'} emri bulunamadı, pozisyon kapatılıyor...`);
                try {
                    const orderParams = {
                        symbol: position.symbol,
                        type: 'MARKET',
                        quantity: positionAmt,
                        positionSide: position.positionSide,
                        side: position.positionSide === 'LONG' ? 'SELL' : 'BUY'
                    };
                    await client.futuresOrder(orderParams);
                    await bot.sendMessage(
                        user.chat_id,
                        `⚠️ ${position.symbol} ${position.positionSide} pozisyonunuz için ${!hasStopLoss ? 'Stop Loss' : 'Take Profit'} emri bulunamadığından pozisyon güvenlik nedeniyle kapatıldı. (Hata Değildir.)`
                    );
                } catch (error) {
                    console.error(`[OrderTracker] Pozisyon kapatma hatası:`, error);
                }
            }
        }
    }
}
    
                    // Pozisyonsuz emirleri kontrol et
                    for (const [key, orders] of orderCache) {
                        const [symbol, positionSide] = key.split('_');
                        const position = positionCache.get(key);
                        
                        if (!position || parseFloat(position.positionAmt) === 0) {
                            console.log(`[OrderTracker] ${symbol} ${positionSide} pozisyon yok, emirler temizleniyor...`);
                            for (const order of orders) {
                                try {
                                    const currentOrder = await client.futuresGetOrder({
                                        symbol: order.symbol,
                                        orderId: order.orderId
                                    }).catch(() => null);
    
                                    if (currentOrder && currentOrder.status === 'NEW') {
                                        await client.futuresCancelOrder({
                                            symbol: order.symbol,
                                            orderId: order.orderId
                                        });
                                    }
                                } catch (error) {
                                    if (error.code === -2011) {
                                        console.log(`[OrderTracker] Emir zaten silinmiş: ${symbol} ${order.orderId}`);
                                    } else {
                                        console.error(`[OrderTracker] Emir iptal hatası:`, error);
                                    }
                                }
                            }
                            
                        }
                    }
                } catch (error) {
                    console.error(`[OrderTracker] Kullanıcı kontrolünde hata: ${user.chat_id}`, error);
                }
            }
    
            // ActiveOrders kontrolü
            for (const [key, orderData] of this.activeOrders.entries()) {
                try {
                    const { chatId, symbol, side } = orderData;
                    console.log(`[OrderTracker] ${symbol} ${side} kontrolü başlıyor...`);
                    
                    const userApiKeys = await dbQuery(`
                        SELECT api_key, api_secret 
                        FROM user_api_keys 
                        WHERE user_id = ?
                    `, [chatId]);
    
                    if (!userApiKeys || !userApiKeys[0]) {
                        ordersToRemove.push({ key, reason: 'API_KEYS_NOT_FOUND', chatId, symbol, side });
                        continue;
                    }
    
                    const decryptedApiKey = decrypt(userApiKeys[0].api_key);
                    const decryptedApiSecret = decrypt(userApiKeys[0].api_secret);
    
                    if (!decryptedApiKey || !decryptedApiSecret) {
                        ordersToRemove.push({ key, reason: 'API_KEYS_NOT_FOUND', chatId, symbol, side });
                        continue;
                    }
    
                    const client = Binance({
                        apiKey: decryptedApiKey,
                        apiSecret: decryptedApiSecret,
                        futures: true
                    });
    
                    const hasPosition = await checkPositionExists(client, symbol, side);
                    
                    if (!hasPosition.exists) {
                        ordersToRemove.push({ key, reason: 'NO_POSITION_ORDERS_CLEARED', chatId, symbol, side });
                        continue;
                    }
    
                    const openOrders = await client.futuresOpenOrders({ symbol });
                    const orderIds = [orderData.tp1, orderData.tp2, orderData.sl].filter(Boolean);
                    const activeOrderIds = openOrders.map(o => o.orderId.toString());
    
                    if (orderIds.length > 0) {
                        const missingOrders = orderIds.filter(id => !activeOrderIds.includes(id.toString()));
                        
                        if (missingOrders.length > 0) {
                            const allOrders = await client.futuresAllOrders({ symbol });
                            
                            for (const missingOrderId of missingOrders) {
                                const order = allOrders.find(o => o.orderId.toString() === missingOrderId.toString());
                                
                                if (!order && (missingOrderId === orderData.sl || missingOrderId === orderData.tp2)) {
                                    console.log(`[OrderTracker] ${symbol} için ${missingOrderId} (${missingOrderId === orderData.sl ? 'SL' : 'TP2'}) bulunamadı, tetiklenmiş kabul ediliyor`);
                                    ordersToRemove.push({ 
                                        key, 
                                        reason: 'ORDER_ASSUMED_EXECUTED',
                                        chatId,
                                        symbol,
                                        side,
                                        executedOrderId: missingOrderId,
                                        orderType: missingOrderId === orderData.sl ? 'STOP_MARKET' : 'TAKE_PROFIT_MARKET'
                                    });
                                    break;
                                }
                                else if (order && order.status === 'FILLED') {
                                    ordersToRemove.push({ 
                                        key, 
                                        reason: 'ORDER_EXECUTED',
                                        chatId,
                                        symbol,
                                        side,
                                        executedOrderId: missingOrderId,
                                        orderType: order.type
                                    });
                                    break;
                                }
                            }
                        }
                    }
                } catch (error) {
                    console.error(`[OrderTracker] ${key} kontrolünde hata:`, error);
                }
            }
    
// Temizleme ve bildirim
for (const orderInfo of ordersToRemove) {
    try {
        const { key, reason, chatId, symbol, side } = orderInfo;
        
        this.activeOrders.delete(key);
        await this.persistToDisk();
        
        if (chatId) {
            // Default mesaj değeri
            let message = `ℹ️ ${symbol} ${side} pozisyonu güncellendi.`;

            switch (reason) {
                case 'API_KEYS_NOT_FOUND':
                    message = `🚫 ${symbol} ${side} pozisyonu için API anahtarları bulunamadı.`;
                    break;
                case 'NO_POSITION_ORDERS_CLEARED':
                    message = `🧹 ${symbol} ${side} pozisyonunuz bulunmadığı için tüm emirler temizlendi.`;
                    break;
                case 'ORDER_EXECUTED':
                    message = `✅ ${symbol} ${side} pozisyonu için emir kontrolü başarıyla gerçekleşti.`;
                    break;
                case 'ORDER_ASSUMED_EXECUTED':
                    const assumedTypeText = orderInfo.orderType === 'STOP_MARKET' ? 'Stop Loss' : 'Take Profit 2';
                    message = `ℹ️ ${symbol} ${side} pozisyonunuz için ${assumedTypeText} emri bulunamadı ve tetiklenmiş kabul edildi.`;
                    break;
            }

            // Mesaj boş değilse gönder
            if (message && message.trim()) {
                try {
                    await bot.sendMessage(chatId, message);
                } catch (error) {
                    console.error(`[OrderTracker] Bildirim gönderme hatası: ${chatId}`, error);
                }
            } else {
                console.log(`[OrderTracker] Boş mesaj oluştu: ${reason} için mesaj tanımlanmamış`);
            }
        }
    } catch (error) {
        console.error(`[OrderTracker] Order temizleme hatası:`, error);
    }
}
    
            if (ordersToRemove.length > 0) {
                console.log(`[OrderTracker] ${ordersToRemove.length} emir temizlendi`);
            }
        } catch (error) {
            console.error('[OrderTracker] ValidateAllOrders ana hata:', error);
        }
    }


    getOrdersForPosition(chatId, symbol, side) {
        const key = `${chatId}_${symbol}_${side}`;
        return this.activeOrders.get(key);
    }

    getOrderInfo(orderId) {
        if (!orderId) return null;
        
        console.log('Checking orderId:', orderId, 'in active orders:', Array.from(this.activeOrders.entries()));
        
        for (const [key, data] of this.activeOrders.entries()) {
            // toString() ile karşılaştırma yapalım ve debug ekleyelim
            const tp1Match = data.tp1?.toString() === orderId.toString();
            const tp2Match = data.tp2?.toString() === orderId.toString();
            const slMatch = data.sl?.toString() === orderId.toString();
            
            console.log('Order matches:', {
                key,
                orderId,
                tp1: data.tp1,
                tp2: data.tp2,
                sl: data.sl,
                tp1Match,
                tp2Match,
                slMatch
            });
    
            if (tp1Match) {
                console.log('Found TP1 match');
                return { ...data, type: 'TP1' };
            }
            if (tp2Match) {
                console.log('Found TP2 match');
                return { ...data, type: 'TP2' };
            }
            if (slMatch) {
                console.log('Found SL match');
                return { ...data, type: 'SL' };
            }
        }
        
        console.log('No matching order found for orderId:', orderId);
        return null;
    }
    
    async saveOrders(chatId, symbol, side, orders) {
        const key = `${chatId}_${symbol}_${side}`;
        const orderData = {
            chatId,
            symbol,
            side,
            tp1: orders.tp1?.toString(), // toString() eklendi
            tp2: orders.tp2?.toString(), // toString() eklendi
            sl: orders.sl?.toString(),   // toString() eklendi
            timestamp: Date.now()
        };
        
        console.log(`Saving orders for ${key}:`, orderData);
        this.activeOrders.set(key, orderData);
        await this.persistToDisk();
        console.log(`[OrderTracker] Emirler kaydedildi ${key}:`, orderData);
    }

    async removeOrders(chatId, symbol, side) {
        const key = `${chatId}_${symbol}_${side}`;
        const removed = this.activeOrders.delete(key);
        if (removed) {
            await this.persistToDisk();
            console.log(`[OrderTracker] Emirler silindi ${key}`);
        }
    }

    async persistToDisk() {
        try {
            await fs.mkdir(this.dataDir, { recursive: true });
            const orderData = Object.fromEntries(this.activeOrders);
            await fs.writeFile(this.orderStorageFile, JSON.stringify(orderData, null, 2));
        } catch (error) {
            console.error('[OrderTracker] Kaydetme hatası:', error);
            throw error;
        }
    }
}

const orderTracker = new OrderTracker();
module.exports = orderTracker;