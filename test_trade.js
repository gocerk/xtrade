const Binance = require('binance-api-node').default;

// ==========================================
// KULLANICI AYARLARI / USER SETTINGS
// ==========================================
const API_KEY = 'oFFem7v0jTfQ7HmXauuyRGspxjNbN8LC9gZGiMuLGhfXFT8Tb471TK0muUGwQYX5';
const API_SECRET = 'PNpVJuf047SAtOaeW1JtjbqxNQUzIPRuq1IPt4A5tW8dNxWpKttihbLpBgACjErY';

const SYMBOL = 'XRPUSDT'; 
const QUANTITY = 10;   
const LEVERAGE = 5;       
// ==========================================

const client = Binance({
  apiKey: API_KEY,
  apiSecret: API_SECRET,
  futures: true
});

async function testTrade() {
  try {
    console.log('Testing Binance connection...');
    
    // 1. Bakiye Kontrolü
    const accountInfo = await client.futuresAccountBalance();
    const usdtBalance = accountInfo.find(b => b.asset === 'USDT');
    console.log(`Wallet Balance: ${parseFloat(usdtBalance.balance).toFixed(2)} USDT`);
    
    // 2. Kaldıraç Ayarlama
    console.log(`Setting leverage to ${LEVERAGE}x for ${SYMBOL}...`);
    await client.futuresLeverage({
      symbol: SYMBOL,
      leverage: LEVERAGE
    });

    // 3. Pozisyon Modu Kontrolü (Hedge vs One-Way)
    // Bu hatayı çözmek için positionSide parametresi eklememiz gerekebilir
    // Eğer Hedge modundaysanız 'LONG' veya 'SHORT' belirtmelisiniz.
    // Eğer One-Way modundaysanız 'BOTH' olmalı (default).
    
    // Çözüm: Hedge modu destekli parametreler ile işlem açma
    
    // 3. BUY (LONG) Order Test
    console.log(`Placing MARKET BUY order for ${QUANTITY} ${SYMBOL}...`);
    const buyOrder = await client.futuresOrder({
      symbol: SYMBOL,
      side: 'BUY',
      type: 'MARKET',
      quantity: QUANTITY,
      // HEDGE Modu desteği için:
      // Long açarken: positionSide: 'LONG'
      // Short açarken: positionSide: 'SHORT'
      // One-way modunda bu parametre genellikle 'BOTH' olarak kabul edilir veya gönderilmez.
      // Hatayı alıyorsanız muhtemelen Hedge modundasınız.
      positionSide: 'LONG' 
    });
    console.log('BUY Order Result:', buyOrder);

    // 4. SELL (SHORT/CLOSE) Order Test
    console.log('Waiting 2 seconds before closing...');
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log(`Placing MARKET SELL order to close position...`);
    const sellOrder = await client.futuresOrder({
      symbol: SYMBOL,
      side: 'SELL',
      type: 'MARKET',
      quantity: QUANTITY,
      // Pozisyon kapatmak için aynı positionSide kullanılmalı
      positionSide: 'LONG' 
    });
    console.log('SELL Order Result:', sellOrder);

    console.log('Test completed successfully!');

  } catch (error) {
    console.error('Test failed:', error.message);
    if (error.code === -4061) {
       console.log("\nTIP: This error usually means your account is in 'One-Way Mode' but we sent 'Hedge Mode' parameters, OR vice versa.");
       console.log("Try changing the 'positionSide' parameter in the script to 'BOTH' or remove it, OR change your Binance settings.");
    }
  }
}

testTrade();
