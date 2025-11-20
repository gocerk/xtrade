const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');
const { encryptionKey, dbQuery } = require('./db');

const signalsFilePath = path.join(__dirname, 'signals.json');

function encrypt(text) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', encryptionKey, iv);
  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  return iv.toString('hex') + ':' + encrypted;
}

function decrypt(text) {
  const textParts = text.split(':');
  const iv = Buffer.from(textParts.shift(), 'hex');
  const encryptedText = textParts.join(':');
  const decipher = crypto.createDecipheriv('aes-256-cbc', encryptionKey, iv);
  let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return decrypted;
}

async function getUserApiKeys(chatId) {
  try {
    const results = await dbQuery('SELECT api_key, api_secret FROM user_api_keys WHERE user_id = ?', [chatId]);
    if (results.length > 0) {
      return {
        apiKey: decrypt(results[0].api_key),
        apiSecret: decrypt(results[0].api_secret)
      };
    }
    return null;
  } catch (error) {
    console.error('Error getting user API keys:', error);
    throw error;
  }
}



async function saveSignal(action, coin, price, technicalIndicators, historical_prices) {
  try {
    let signals = [];
    try {
      const data = await fs.readFile(signalsFilePath, 'utf8');
      signals = JSON.parse(data);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        console.error('Error reading signals.json:', error);
      }
    }

    signals.push({
      action,
      coin,
      price,
      technicalIndicators: {
        ema: technicalIndicators?.ema || null,
        macd: technicalIndicators?.macd || null,
        macd_signal: technicalIndicators?.macd_signal || null,
        rsi: technicalIndicators?.rsi || null
      },
      historical_prices: historical_prices || null,
      timestamp: new Date().toISOString()
    });

    if (signals.length > 10) {
      signals.shift();
    }

    await fs.writeFile(signalsFilePath, JSON.stringify(signals, null, 2));
  } catch (error) {
    console.error('Error saving signal:', error);
    throw error;
  }
}

async function getLast10Signals() {
  try {
    const data = await fs.readFile(signalsFilePath, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }
    console.error('Error reading signals.json:', error);
    throw error;
  }
}

async function checkTradingLimits(leverage, amount) {
  try {
      const limits = await dbQuery('SELECT * FROM trading_limits ORDER BY id DESC LIMIT 1');
      if (limits.length === 0) return true;

      if (leverage > limits[0].max_leverage) {
          throw new Error(`Maksimum kaldıraç limiti: ${limits[0].max_leverage}x`);
      }
      
      if (amount > limits[0].max_usdt) {
          throw new Error(`Maksimum USDT limiti: ${limits[0].max_usdt} USDT`);
      }

      return true;
  } catch (error) {
      throw error;
  }
}

function getDecimalPlaces(num) {
  const match = ('' + num).match(/(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/);
  if (!match) return 0;
  return Math.max(0, (match[1] ? match[1].length : 0) - (match[2] ? +match[2] : 0));
}

function isJSONString(str) {
  try {
    JSON.parse(str);
    return true;
  } catch (error) {
    return false;
  }
}

function calculateLeveragedValues(amount, leverage, stopLossPercent, takeProfitPercent1, takeProfitPercent2) {
  const leveragedAmount = amount * leverage;
  const adjustedStopLoss = stopLossPercent / leverage;
  const adjustedTakeProfit1 = takeProfitPercent1 / leverage;
  const adjustedTakeProfit2 = takeProfitPercent2 / leverage;
  
  console.log('Calculated values:', {
    leveragedAmount,
    adjustedStopLoss,
    adjustedTakeProfit1,
    adjustedTakeProfit2
  });

  return {
    leveragedAmount,
    adjustedStopLoss,
    adjustedTakeProfit1,
    adjustedTakeProfit2
  };
}

module.exports = {
  calculateLeveragedValues,
  saveSignal,
  getLast10Signals,
  getDecimalPlaces,
  isJSONString,
  encrypt,
  decrypt,
  getUserApiKeys,
  checkTradingLimits
};