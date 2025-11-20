const { dbQuery } = require('./db');
const { getMessage } = require('./lang');
const { encrypt, decrypt } = require('./utils');
const Binance = require('binance-api-node').default;

async function isAdmin(chatId) {
  try {
    const results = await dbQuery("SELECT * FROM admins WHERE id = ?", [chatId]);
    return results.length > 0;
  } catch (err) {
    console.error('Error checking admin status:', err);
    return false;
  }
}

async function getLicenseKey(key) {
  try {
    const results = await dbQuery("SELECT * FROM license_keys WHERE license_key = ?", [key]);
    if (results[0]) {
      if (results[0].used == false) {
        await dbQuery("UPDATE license_keys SET used = 1 WHERE license_key = ?", [key]);
        return { status: true, message: "Anahtar bulundu ve kullanıldı", day: results[0].time };
      } else {
        return { status: false, message: getMessage('keyAlreadyUsed') };
      }
    } else {
      return { status: false, message: getMessage('keyNotFound') };
    }
  } catch (err) {
    console.error('Error getting license key:', err);
    return { status: false, message: getMessage('errorOccurred') };
  }
}

async function addDayToUser(daysToAdd, userId) {
  try {
    const resultsRaw = await dbQuery('SELECT time FROM bultende_olanlar WHERE user_id = ?', [userId]);
    const results = Array.isArray(resultsRaw) ? resultsRaw : [];
    
    let newEndTime;
    let sql;
    let queryParams;

    const now = new Date();

    if (results.length > 0) {
      // Mevcut en son aboneliği bul
      const latestSubscription = results.reduce((latest, current) => {
        return new Date(current.time) > new Date(latest.time) ? current : latest;
      }, results[0]);
      
      const currentEndDate = new Date(latestSubscription.time);
      
      // Eğer abonelik sona ermediyse, mevcut aboneliğin üzerine ekle
      if (currentEndDate > now) {
        newEndTime = new Date(currentEndDate);
        newEndTime.setDate(newEndTime.getDate() + parseInt(daysToAdd));
        
        // MySQL datetime formatına çevir
        const formattedDate = formatMySQLDate(newEndTime);
        
        sql = 'UPDATE bultende_olanlar SET time = ? WHERE user_id = ? AND time = ?';
        queryParams = [formattedDate, userId, formatMySQLDate(currentEndDate)];
      } else {
        // Abonelik sona ermişse, yeni bir abonelik oluştur
        newEndTime = new Date();
        newEndTime.setDate(newEndTime.getDate() + parseInt(daysToAdd));
        
        // MySQL datetime formatına çevir
        const formattedDate = formatMySQLDate(newEndTime);
        
        sql = 'INSERT INTO bultende_olanlar (user_id, time) VALUES (?, ?)';
        queryParams = [userId, formattedDate];
      }
    } else {
      // Hiç abonelik yoksa, yeni bir abonelik oluştur
      newEndTime = new Date();
      newEndTime.setDate(newEndTime.getDate() + parseInt(daysToAdd));
      
      // MySQL datetime formatına çevir
      const formattedDate = formatMySQLDate(newEndTime);
      
      sql = 'INSERT INTO bultende_olanlar (user_id, time) VALUES (?, ?)';
      queryParams = [userId, formattedDate];
    }

    await dbQuery(sql, queryParams);
    
    return formatDisplayDate(newEndTime);
  } catch (err) {
    console.error('Error adding days to user:', err);
    throw err;
  }
}

// Helper fonksiyonu: MySQL datetime formatına çevirme
function formatMySQLDate(date) {
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

// Helper fonksiyonu: Görüntüleme için tarih formatı
function formatDisplayDate(date) {
  return date.toLocaleString('tr-TR', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

// checkIsUserSubscriber fonksiyonu - kullanıcının aktif aboneliği varmı kontrol et
async function checkIsUserSubscriber(userId) {
  try {
    // Veritabanında bultende_olanlar tablosunda user_id ile eşleşen kayıt ara
    const resultRaw = await dbQuery('SELECT time FROM bultende_olanlar WHERE user_id = ?', [userId]);
    const result = Array.isArray(resultRaw) ? resultRaw : [];
    
    // Sonuç yoksa, kullanıcı abone değil
    if (result.length === 0) {
      return { isSubscriber: false, endTime: null };
    }
    
    // Son abonelik tarihini al
    const lastSubscription = result.reduce((latest, current) => {
      return new Date(current.time) > new Date(latest.time) ? current : latest;
    }, result[0]);
    
    // Abonelik tarihi gelecekte mi?
    const endTime = new Date(lastSubscription.time);
    const isSubscriber = endTime > new Date();
    
    // Bitiş tarihini formatla
    const formattedEndTime = formatDisplayDate(endTime);
    
    return { isSubscriber, endTime: formattedEndTime };
  } catch (err) {
    console.error('Error checking user subscription:', err);
    throw err;
  }
}

async function getUserBinanceClient(userId) {
  try {
    const results = await dbQuery('SELECT api_key, api_secret FROM user_api_keys WHERE user_id = ?', [userId]);
    if (results.length === 0) {
      throw new Error(getMessage('apiKeysNotFound'));
    }

    let apiKey, apiSecret;
    try {
      apiKey = decrypt(results[0].api_key);
      apiSecret = decrypt(results[0].api_secret);
      console.log('Decrypted API Key:', apiKey ? 'Defined' : 'Undefined');
      console.log('Decrypted API Secret:', apiSecret ? 'Defined' : 'Undefined');
    } catch (e) {
      console.error('Error decrypting API keys:', e);
      throw new Error(getMessage('apiKeysError'));
    }

    const client = Binance({
      apiKey: apiKey,
      apiSecret: apiSecret,
      futures: true
    });

    return client;
  } catch (err) {
    console.error('Error getting Binance client:', err);
    throw err;
  }
}

module.exports = {
  isAdmin,
  getLicenseKey,
  addDayToUser,
  checkIsUserSubscriber,
  getUserBinanceClient
};