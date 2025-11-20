const { handleDatabaseConnection } = require('./db');
const express = require('express');
const session = require('express-session');
const MySQLStore = require('express-mysql-session')(session);
const path = require('path');
const bodyParser = require('body-parser');
const flash = require('connect-flash');
const axios = require('axios');
const os = require('os');
const { validateSecurity } = require('./utils/security');
const fs = require('fs').promises;
const orderTracker = require('./orderTracker');
const http = require('http');
const WebSocket = require('ws');
const expressLayouts = require('express-ejs-layouts');
const { startPolling, stopPolling } = require('./bot');
const crypto = require('crypto');
const { isJSONString, getLast10Signals, getDecimalPlaces, encrypt, decrypt, calculateLeveragedValues, checkTradingLimits } = require('./utils');
const { testBinanceAPI, executeTrade, autoTradeUsers, startUserStream, stopUserStream, getUserNotificationStatus, updateUserNotificationStatus, closeAllPositionsAndOrders } = require('./binance_operations');
const { isAdmin, getLicenseKey, addDayToUser, checkIsUserSubscriber, getUserBinanceClient } = require('./user_management');
const { showPositions, closePosition } = require('./positions');
const moment = require('moment');
const { pool, dbQuery } = require('./db');
const { saveSignal } = require('./utils');
const { sendMessageToSubscribers } = require('./bot');

// .env dosyasını yükle
require('dotenv').config();

// Port ayarları
const PORT = process.env.PORT || 3000;
const RESTART_INTERVAL = 6 * 60 * 60 * 1000; // 6 saat

// Dashboard uygulaması oluştur
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server: server });

// ÖNEMLİ: Body-parser'ı en başta uygula
// Bu kısım tüm route tanımlamalarından önce olmalı
app.use(bodyParser.json({ limit: '10mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '10mb' }));

// Session store için veritabanı bağlantı bilgileri
const sessionOptions = {
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'traderprobot',
  clearExpired: true,
  checkExpirationInterval: 900000, // 15 dakikada bir expire kontrolü
  expiration: 86400000, // 1 gün
  createDatabaseTable: true,
  schema: {
    tableName: 'sessions',
    columnNames: {
      session_id: 'id',
      expires: 'expires',
      data: 'data'
    }
  }
};

// Session store oluştur
const sessionStore = new MySQLStore(sessionOptions);

// Aktif WebSocket bağlantılarını tutacak global değişken
const clientConnections = new Map();

// WebSocket bağlantı yönetimi
wss.on('connection', (ws, req) => {
  // URL'den session ID'sini almak için (örnek: /ws?sessionId=xxx)
  const url = new URL(req.url, `http://${req.headers.host}`);
  const sessionId = url.searchParams.get('sessionId');
  
  if (!sessionId) {
    ws.close(1008, 'Session ID required');
    return;
  }
  
  // Session'dan kullanıcı ID'sini alma
  sessionStore.get(sessionId, (err, session) => {
    if (err || !session || !session.userId) {
      ws.close(1008, 'Invalid session');
      return;
    }
    
    const userId = session.userId;
    
    // Kullanıcı için WebSocket bağlantısını kaydet
    if (!clientConnections.has(userId)) {
      clientConnections.set(userId, new Set());
    }
    clientConnections.get(userId).add(ws);
    
    console.log(`WebSocket connected for user: ${userId}`);
    
    // Ping/Pong ile bağlantıyı canlı tut
    const pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, 30000);
    
    ws.on('close', () => {
      console.log(`WebSocket closed for user: ${userId}`);
      clearInterval(pingInterval);
      
      // Kullanıcının bağlantılarından bu WebSocket'i kaldır
      const userConnections = clientConnections.get(userId);
      if (userConnections) {
        userConnections.delete(ws);
        if (userConnections.size === 0) {
          clientConnections.delete(userId);
        }
      }
    });
    
    ws.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        
        // Özel mesaj işlemleri buraya eklenebilir
        if (data.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong' }));
        }
      } catch (error) {
        console.error('WebSocket message error:', error);
      }
    });
    
    // Kullanıcıya hoş geldin mesajı gönder
    ws.send(JSON.stringify({ 
      type: 'info', 
      message: 'WebSocket bağlantısı başarıyla kuruldu.' 
    }));
  });
});

// Kullanıcıya bildirim gönderme yardımcı fonksiyonu
async function sendNotification(userId, message, type = 'info') {
  try {
    // Bildirim veritabanına kaydedilir
    await dbQuery(
      'INSERT INTO notifications (user_id, message, type) VALUES (?, ?, ?)',
      [userId, message, type]
    );
    
    // Kullanıcı aktif bağlantılarında ise bildirim anında gönderilir
    if (clientConnections.has(userId)) {
      const notification = {
        type: 'notification',
        messageType: type,
        message,
        timestamp: new Date().toISOString()
      };
      
      clientConnections.get(userId).forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(notification));
        }
      });
    }
  } catch (error) {
    console.error(`Error sending notification to user ${userId}:`, error);
  }
}

// Tüm kullanıcılara bildirim gönderme fonksiyonu
async function broadcastNotification(message, type = 'info') {
  try {
    // Aktif aboneleri al
    const subscribers = await dbQuery("SELECT user_id FROM bultende_olanlar WHERE time > NOW()");
    
    for (const subscriber of subscribers) {
      await sendNotification(subscriber.user_id, message, type);
    }
  } catch (error) {
    console.error('Error broadcasting notification:', error);
  }
}

// IP kontrol fonksiyonu
async function getCurrentIP() {
  const networkInterfaces = os.networkInterfaces();
  for (const interfaceName of Object.keys(networkInterfaces)) {
    const interface = networkInterfaces[interfaceName];
    for (const addr of interface) {
      if (addr.family === 'IPv4' && !addr.internal) {
        return addr.address;
      }
    }
  }
  throw new Error('Could not determine server IP address');
}

async function checkIPAuthorization() {
  try {
    const currentIP = await getCurrentIP();
    console.log('[TraderProBot] Current IP:', currentIP);

    const response = await axios.get('https://webza.net/licenses/allowed_ips.json');
    const allowedIPs = response.data.ips;

    if (!allowedIPs.includes(currentIP)) {
      console.error('[TraderProBot] Unauthorized IP address:', currentIP);
      return false;
    }
    
    console.log('[TraderProBot] IP authorization successful');
    return true;
  } catch (error) {
    console.error('[TraderProBot] License check failed:', error.message);
    return false;
  }
}

// Şifre hash fonksiyonu
function hashPassword(password) {
  return crypto.createHash('sha256').update(password).digest('hex');
}

// Gerekli veritabanı tablolarını kontrol et
async function ensureDatabaseTables() {
  try {
    // sessions tablosunu kontrol et ve oluştur
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS sessions (
        id varchar(128) NOT NULL PRIMARY KEY,
        data text NOT NULL,
        expires int(11) NOT NULL
      )
    `);

    // notifications tablosunu kontrol et ve oluştur
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS notifications (
        id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id int(11) NOT NULL,
        message text NOT NULL,
        is_read tinyint(1) DEFAULT 0,
        type varchar(50) DEFAULT 'info',
        created_at timestamp NULL DEFAULT current_timestamp(),
        KEY idx_user_id (user_id),
        KEY idx_is_read (is_read)
      )
    `);

    // FREE TIER: free_users tablosu
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS free_users (
        user_id int(11) NOT NULL PRIMARY KEY,
        created_at timestamp NULL DEFAULT current_timestamp(),
        analiz_count int(11) NOT NULL DEFAULT 0,
        last_analiz_at datetime NULL
      )
    `);

    // FREE TIER: system counters
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS system_counters (
        name varchar(64) NOT NULL PRIMARY KEY,
        value bigint NOT NULL DEFAULT 0
      )
    `);



    // Tabloda user_id sütunu var mı kontrol et
    try {
      const columns = await dbQuery(`
        SHOW COLUMNS FROM bultende_olanlar LIKE 'user_id'
      `);
      
      // Eğer user_id sütunu yoksa, id sütununu user_id olarak değiştir
      if (columns.length === 0) {
        console.log('Updating bultende_olanlar table structure...');
        try {
          await dbQuery(`
            ALTER TABLE bultende_olanlar CHANGE COLUMN id user_id int(11) DEFAULT NULL
          `);
          console.log('Successfully updated bultende_olanlar table');
        } catch (e) {
          // Eğer id sütunu yoksa yeni bir sütun ekle
          console.log('Adding user_id column to bultende_olanlar table...');
          await dbQuery(`
            ALTER TABLE bultende_olanlar ADD COLUMN user_id int(11) DEFAULT NULL
          `);
          console.log('Successfully added user_id column to bultende_olanlar table');
        }
      }
    } catch (e) {
      console.log('Error checking bultende_olanlar table structure:', e);
    }

    // user_api_keys tablosunu kontrol et
    try {
      const columns = await dbQuery(`
        SHOW COLUMNS FROM user_api_keys WHERE Field = 'user_id'
      `);
      
      if (columns.length > 0 && columns[0].Type !== 'int(11)') {
        await dbQuery(`
          ALTER TABLE user_api_keys MODIFY COLUMN user_id int(11) NOT NULL
        `);
        console.log('Modified user_api_keys.user_id to int(11)');
      }
    } catch (e) {
      console.log('Error checking/updating user_api_keys table:', e);
    }

    // user_settings tablosunu kontrol et
    try {
      const columns = await dbQuery(`
        SHOW COLUMNS FROM user_settings WHERE Field = 'user_id'
      `);
      
      if (columns.length > 0 && columns[0].Type !== 'int(11)') {
        await dbQuery(`
          ALTER TABLE user_settings MODIFY COLUMN user_id int(11) NOT NULL
        `);
        console.log('Modified user_settings.user_id to int(11)');
      }
    } catch (e) {
      console.log('Error checking/updating user_settings table:', e);
    }

    console.log('[TraderProBot] Veritabanı tabloları kontrol edildi ve oluşturuldu');
  } catch (error) {
    console.error('[TraderProBot] Veritabanı tabloları kontrolü sırasında hata:', error);
  }
}

// Admin kullanıcı kontrolü ve oluşturma
async function ensureAdminUser() {
  try {
    const admins = await dbQuery('SELECT * FROM users WHERE is_admin = 1');
    
    if (admins.length === 0) {
      const adminPassword = 'E3Z98270*M';
      const hashedPassword = hashPassword(adminPassword);
      
      await dbQuery(
        'INSERT INTO users (username, password, email, is_admin) VALUES (?, ?, ?, ?)',
        ['admin', hashedPassword, 'admin@traderprobot.com', 1]
      );
      
      console.log('[TraderProBot] Admin kullanıcısı oluşturuldu. Kullanıcı adı: admin, Şifre: E3Z98270*M');
    }
  } catch (error) {
    console.error('[TraderProBot] Admin kullanıcı kontrolü sırasında hata:', error);
  }
}

// Web uygulaması konfigürasyonu
function configureWebDashboard() {
  // Middleware
  // BodyParser middleware zaten ana kısımda uygulandı
  app.use(express.static(path.join(__dirname, 'public')));
  
  // Session middleware
  app.use(session({
    key: 'traderprobot_session',
    secret: process.env.SESSION_SECRET || 'traderprobottradesecret',
    store: sessionStore,
    resave: true,
    saveUninitialized: true,
    cookie: { 
      secure: false, // HTTPS kullanmıyorsak false olmalı
      maxAge: 86400000 // 1 gün
    }
  }));
  
  // Flash messages middleware
  app.use(flash());
  
  // EJS layout middleware
  app.use(expressLayouts);
  app.set('layout', 'layout');

  // Template engine
  app.set('view engine', 'ejs');
  app.set('views', path.join(__dirname, 'views'));

  // Local değişkenleri tüm şablonlara ekle
  app.use((req, res, next) => {
    res.locals.user = req.session.user || null;
    res.locals.isAdmin = req.session.isAdmin || false;
    res.locals.error = req.flash('error');
    res.locals.success = req.flash('success');
    res.locals.info = req.flash('info');
    res.locals.currentPath = req.path;
    res.locals.sessionId = req.sessionID;
    next();
  });

  // Kimlik doğrulama kontrolü middleware
  const requireLogin = (req, res, next) => {
    if (!req.session.userId) {
      req.flash('error', 'Bu sayfayı görüntülemek için giriş yapmanız gerekiyor.');
      return res.redirect('/login');
    }
    next();
  };

  // Admin kontrolü middleware
  const requireAdmin = async (req, res, next) => {
    if (!req.session.userId || !req.session.isAdmin) {
      req.flash('error', 'Bu sayfaya erişim yetkiniz yok.');
      return res.redirect('/dashboard');
    }
    next();
  };

  // Abonelik kontrolü middleware
  const requireSubscription = async (req, res, next) => {
    try {
      const userId = req.session.userId;
      const subsinfo = await checkIsUserSubscriber(userId);
      
      if (!subsinfo.isSubscriber) {
        req.flash('error', 'Bu özelliği kullanmak için aktif bir aboneliğe sahip olmanız gerekiyor.');
        return res.redirect('/license');
      }
      
      next();
    } catch (error) {
      console.error('Subscription check error:', error);
      req.flash('error', 'Abonelik kontrolü sırasında bir hata oluştu.');
      res.redirect('/dashboard');
    }
  };

  // Authentication routes
  app.get('/login', (req, res) => {
    if (req.session.userId) {
      return res.redirect('/dashboard');
    }
    res.render('login', { layout: false });
  });

  app.post('/login', async (req, res) => {
    console.log('Login isteği alındı:', req.body);
    
    const { username, password } = req.body;
    
    if (!username || !password) {
      console.log('Kullanıcı adı veya şifre eksik');
      req.flash('error', 'Kullanıcı adı ve şifre gereklidir');
      return res.render('login', { layout: false, username });
    }
    
    try {
      const hashedPassword = hashPassword(password);
      console.log('Hashed password:', hashedPassword);
      
      const users = await dbQuery(
        'SELECT * FROM users WHERE username = ? AND password = ?',
        [username, hashedPassword]
      );
      
      console.log('Bulunan kullanıcı sayısı:', users.length);
      
      if (users.length > 0) {
        const user = users[0];
        console.log('Kullanıcı bulundu:', user.id, user.username);
        
        // Session'a kullanıcı bilgilerini ekle
        req.session.userId = user.id;
        req.session.user = {
          id: user.id,
          username: user.username,
          email: user.email
        };
        req.session.isAdmin = user.is_admin === 1;
        
        // Session kaydını zorla
        req.session.save(async (err) => {
          if (err) {
            console.error('Session kayıt hatası:', err);
            req.flash('error', 'Oturum kaydedilirken bir hata oluştu');
            return res.render('login', { layout: false, username });
          }
          
          console.log('Session kaydedildi, kullanıcı yönlendiriliyor');
          
          // Son giriş zamanını güncelle
          try {
            await dbQuery(
              'UPDATE users SET last_login = NOW() WHERE id = ?',
              [user.id]
            );
          } catch (updateErr) {
            console.error('Son giriş zamanı güncelleme hatası:', updateErr);
            // Bu hata oturumu etkilemez, devam et
          }
          
          return res.redirect('/dashboard');
        });
      } else {
        console.log('Kullanıcı bulunamadı veya şifre yanlış');
        req.flash('error', 'Geçersiz kullanıcı adı veya şifre');
        res.render('login', { layout: false, username });
      }
    } catch (error) {
      console.error('Login error:', error);
      req.flash('error', 'Giriş sırasında bir hata oluştu');
      res.render('login', { layout: false, username });
    }
  });

  app.get('/logout', (req, res) => {
    req.session.destroy(() => {
      res.redirect('/login');
    });
  });

  // Dashboard
  app.get('/', (req, res) => {
    res.redirect('/dashboard');
  });

  app.get('/dashboard', requireLogin, async (req, res) => {
    try {
      const userId = req.session.userId;
      const subsinfo = await checkIsUserSubscriber(userId);
      
      // Son sinyalleri al
      const signals = await dbQuery(`
        SELECT w.id, w.coin, w.action, w.price, w.timeframe, w.created_at,
          (SELECT status FROM signal_results WHERE webhook_log_id = w.id LIMIT 1) as status
        FROM webhook_logs w
        ORDER BY w.created_at DESC
        LIMIT 10
      `);
      
      // Kullanıcının işlem istatistikleri
      const stats = await dbQuery(`
        SELECT 
          COUNT(*) as total_trades,
          SUM(CASE WHEN profit_percentage > 0 THEN 1 ELSE 0 END) as profitable_trades,
          SUM(CASE WHEN profit_percentage <= 0 THEN 1 ELSE 0 END) as losing_trades,
          AVG(profit_percentage) as avg_profit
        FROM signal_results
        WHERE status = 'COMPLETED'
      `);
      
      // Kullanıcının bildirimlerini al
      const notifications = await dbQuery(
        'SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 5',
        [userId]
      );
      
      res.render('dashboard', { 
        title: 'Ana Sayfa',
        userId, 
        isSubscriber: subsinfo.isSubscriber,
        endTime: subsinfo.endTime,
        signals,
        stats: stats[0],
        notifications
      });
    } catch (error) {
      console.error('Dashboard error:', error);
      req.flash('error', 'Dashboard yüklenirken bir hata oluştu');
      res.render('dashboard', { error: 'Veriler yüklenirken bir hata oluştu' });
    }
  });

  // Settings page
  app.get('/settings', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const settings = await dbQuery('SELECT * FROM user_settings WHERE user_id = ?', [userId]);
      const userSettings = settings.length > 0 ? settings[0] : {};
      
      res.render('settings', { 
        title: 'İşlem Ayarları',
        settings: userSettings
      });
    } catch (error) {
      console.error('Settings error:', error);
      req.flash('error', 'Ayarlar yüklenirken bir hata oluştu');
      res.render('settings', { error: 'Ayarlar yüklenirken bir hata oluştu' });
    }
  });

  app.post('/settings', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const { amount_per_trade, leverage, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type } = req.body;
      
      // Validasyon
      if (isNaN(parseFloat(leverage)) || parseFloat(leverage) <= 0) {
        req.flash('error', 'Geçersiz kaldıraç değeri');
        return res.redirect('/settings');
      }
      
      try {
        await checkTradingLimits(parseFloat(leverage), parseFloat(amount_per_trade));
      } catch (error) {
        req.flash('error', error.message);
        return res.redirect('/settings');
      }
      
      // Ayarları kaydet
      await dbQuery(
        `INSERT INTO user_settings 
         (user_id, amount_per_trade, leverage, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type) 
         VALUES (?, ?, ?, ?, ?, ?, ?) 
         ON DUPLICATE KEY UPDATE 
         amount_per_trade = VALUES(amount_per_trade), 
         leverage = VALUES(leverage), 
         profit_percent_1 = VALUES(profit_percent_1), 
         profit_percent_2 = VALUES(profit_percent_2), 
         stop_loss_percent = VALUES(stop_loss_percent),
         margin_type = VALUES(margin_type)`,
        [userId, amount_per_trade, leverage, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type]
      );
      
      // Hesaplamaları yap
      const calculated = calculateLeveragedValues(
        parseFloat(amount_per_trade),
        parseFloat(leverage),
        parseFloat(stop_loss_percent),
        parseFloat(profit_percent_1),
        parseFloat(profit_percent_2)
      );
      
      // Hesaplamaları kaydet
      await dbQuery(
        `UPDATE user_settings 
         SET leveraged_amount = ?, 
             adjusted_stop_loss = ?, 
             adjusted_take_profit_1 = ?, 
             adjusted_take_profit_2 = ? 
         WHERE user_id = ?`,
        [
          calculated.leveragedAmount,
          calculated.adjustedStopLoss,
          calculated.adjustedTakeProfit1,
          calculated.adjustedTakeProfit2,
          userId
        ]
      );
      
      req.flash('success', 'Ayarlarınız başarıyla kaydedildi');
      res.redirect('/settings');
    } catch (error) {
      console.error('Save settings error:', error);
      req.flash('error', 'Ayarlar kaydedilirken bir hata oluştu');
      res.redirect('/settings');
    }
  });

  // API settings
  app.get('/api-settings', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const apiKeys = await dbQuery('SELECT * FROM user_api_keys WHERE user_id = ?', [userId]);
      const hasApiKeys = apiKeys.length > 0;
      
      res.render('api-settings', { 
        title: 'API Ayarları',
        hasApiKeys
      });
    } catch (error) {
      console.error('API settings page error:', error);
      req.flash('error', 'API ayarları yüklenirken bir hata oluştu');
      res.redirect('/dashboard');
    }
  });

  app.post('/api-settings', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const { apiKey, apiSecret } = req.body;
      
      // API anahtarlarını doğrula
      const isValid = await testBinanceAPI(apiKey, apiSecret);
      
      if (isValid) {
        const encryptedApiKey = encrypt(apiKey);
        const encryptedApiSecret = encrypt(apiSecret);
        
        await dbQuery(
          'INSERT INTO user_api_keys (user_id, api_key, api_secret) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE api_key = VALUES(api_key), api_secret = VALUES(api_secret)', 
          [userId, encryptedApiKey, encryptedApiSecret]
        );
        
        req.flash('success', 'API anahtarlarınız başarıyla kaydedildi');
        return res.redirect('/api-settings');
      }
      
      req.flash('error', 'API anahtarları geçersiz');
      res.redirect('/api-settings');
    } catch (error) {
      console.error('API settings error:', error);
      req.flash('error', 'API ayarları kaydedilirken bir hata oluştu');
      res.redirect('/api-settings');
    }
  });

  app.post('/api-settings/delete', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      await dbQuery('DELETE FROM user_api_keys WHERE user_id = ?', [userId]);
      
      req.flash('success', 'API anahtarlarınız başarıyla silindi');
      res.redirect('/api-settings');
    } catch (error) {
      console.error('Delete API keys error:', error);
      req.flash('error', 'API anahtarları silinirken bir hata oluştu');
      res.redirect('/api-settings');
    }
  });

  // Positions
  app.get('/positions', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      // API anahtarları kontrolü
      const client = await getUserBinanceClient(userId);
      if (!client) {
        req.flash('error', 'API anahtarları bulunamadı');
        return res.redirect('/api-settings');
      }
      
      // Pozisyonları al
      const positions = await client.futuresPositionRisk();
      const activePositions = positions.filter(p => parseFloat(p.positionAmt) !== 0);
      
      // Hesap bakiyesini al
      const accountInfo = await client.futuresAccountBalance();
      const walletBalance = accountInfo.find(b => b.asset === 'USDT').balance;
      
      res.render('positions', { 
        title: 'Pozisyonlar',
        positions: activePositions,
        walletBalance: parseFloat(walletBalance).toFixed(2)
      });
    } catch (error) {
      console.error('Positions error:', error);
      req.flash('error', 'Pozisyonlar yüklenirken bir hata oluştu');
      res.redirect('/dashboard');
    }
  });

  app.post('/positions/close', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const { symbol, side } = req.body;
      
      await closePosition(userId, symbol, side);
      
      req.flash('success', `${symbol} pozisyonu başarıyla kapatıldı`);
      res.redirect('/positions');
    } catch (error) {
      console.error('Close position error:', error);
      req.flash('error', `Pozisyon kapatılırken bir hata oluştu: ${error.message}`);
      res.redirect('/positions');
    }
  });

  // Auto-trade settings
  app.get('/auto-trade', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      // Otomatik işlem durumu
      const settings = await dbQuery('SELECT autotrade FROM user_settings WHERE user_id = ?', [userId]);
      const autotrade = settings.length > 0 ? settings[0].autotrade : 0;
      
      // Kullanıcının API anahtarı var mı kontrol et
      const apiKeys = await dbQuery('SELECT * FROM user_api_keys WHERE user_id = ?', [userId]);
      const hasApiKeys = apiKeys.length > 0;
      
      res.render('auto-trade', { 
        title: 'Otomatik İşlem',
        autotrade,
        hasApiKeys
      });
    } catch (error) {
      console.error('Auto-trade error:', error);
      req.flash('error', 'Otomatik işlem ayarları yüklenirken bir hata oluştu');
      res.redirect('/dashboard');
    }
  });

  app.post('/auto-trade/toggle', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      // API anahtarları kontrolü
      const apiKeys = await dbQuery('SELECT * FROM user_api_keys WHERE user_id = ?', [userId]);
      if (apiKeys.length === 0) {
        req.flash('error', 'Otomatik işlem için API anahtarlarınızı eklemeniz gerekiyor');
        return res.redirect('/api-settings');
      }
      
      // İşlem ayarları kontrolü
      const settings = await dbQuery('SELECT * FROM user_settings WHERE user_id = ?', [userId]);
      if (settings.length === 0 || !settings[0].amount_per_trade || !settings[0].leverage) {
        req.flash('error', 'Otomatik işlem için işlem ayarlarınızı yapılandırmanız gerekiyor');
        return res.redirect('/settings');
      }
      
      // Mevcut durumu al
      const autotrade = settings.length > 0 ? !settings[0].autotrade : true;
      
      // Güncelle
      await dbQuery(
        'INSERT INTO user_settings (user_id, autotrade) VALUES (?, ?) ON DUPLICATE KEY UPDATE autotrade = ?', 
        [userId, autotrade, autotrade]
      );
      
      // Kullanıcı stream'ini başlat/durdur
      if (autotrade) {
        await startUserStream(userId);
        req.flash('success', 'Otomatik işlem başarıyla aktifleştirildi');
      } else {
        await stopUserStream(userId);
        req.flash('success', 'Otomatik işlem başarıyla devre dışı bırakıldı');
      }
      
      res.redirect('/auto-trade');
    } catch (error) {
      console.error('Toggle auto-trade error:', error);
      req.flash('error', 'Otomatik işlem durumu değiştirilirken bir hata oluştu');
      res.redirect('/auto-trade');
    }
  });

// Tüm Sinyaller sayfası
// Tüm Sinyaller sayfası
app.get('/all-signals', requireLogin, async (req, res) => {
  try {
    const userId = req.session.userId;
    const subsinfo = await checkIsUserSubscriber(userId);
    
    // Filtreleme parametreleri
    const page = parseInt(req.query.page) || 1;
    const limit = 15; // Sayfa başına 15 kayıt
    const offset = (page - 1) * limit;
    
    const coin = req.query.coin || '';
    const action = req.query.action || '';
    const status = req.query.status || '';
    const dateRange = req.query.dateRange || 'all';
    const sortOption = req.query.sort || 'newest';
    
    // SQL sorgusu için WHERE koşulları
    let whereConditions = [];
    let queryParams = [];
    
    if (coin) {
      whereConditions.push('wl.coin = ?');
      queryParams.push(coin);
    }
    
    if (action) {
      whereConditions.push('wl.action = ?');
      queryParams.push(action);
    }
    
    if (status) {
      whereConditions.push('sr.status = ?');
      queryParams.push(status);
    }
    
    // Tarih filtresi
    if (dateRange !== 'all') {
      const now = new Date();
      let startDate;
      
      if (dateRange === 'today') {
        startDate = new Date(now.setHours(0, 0, 0, 0));
      } else if (dateRange === 'week') {
        startDate = new Date(now);
        startDate.setDate(startDate.getDate() - 7);
      } else if (dateRange === 'month') {
        startDate = new Date(now);
        startDate.setDate(startDate.getDate() - 30);
      }
      
      if (startDate) {
        whereConditions.push('sr.created_at >= ?');
        queryParams.push(startDate);
      }
    }
    
    // WHERE cümlesini oluştur
    const whereClause = whereConditions.length > 0 
      ? 'WHERE ' + whereConditions.join(' AND ')
      : '';
    
    // Sıralama parametresini belirle
    let orderBy;
    switch(sortOption) {
      case 'oldest':
        orderBy = 'sr.created_at ASC';
        break;
      case 'coin_asc':
        orderBy = 'wl.coin ASC';
        break;
      case 'coin_desc':
        orderBy = 'wl.coin DESC';
        break;
      case 'profit_desc':
        orderBy = 'sr.profit_percentage DESC';
        break;
      case 'profit_asc':
        orderBy = 'sr.profit_percentage ASC';
        break;
      case 'newest':
      default:
        orderBy = 'sr.created_at DESC';
        break;
    }
    
    // Toplam kayıt sayısını al
    const countQuery = `
      SELECT COUNT(*) as total 
      FROM signal_results sr
      JOIN webhook_logs wl ON sr.webhook_log_id = wl.id 
      ${whereClause}
    `;
    
    console.log('Count Query:', countQuery);
    console.log('Query Params:', queryParams);
    
    const countResult = await dbQuery(countQuery, queryParams);
    console.log('Count Result:', JSON.stringify(countResult));
    
    // Burada countResult'ın yapısını kontrol et
    if (!countResult || !Array.isArray(countResult) || countResult.length === 0) {
      console.error('Count result is invalid:', countResult);
      throw new Error('Invalid count result');
    }

    const totalCount = countResult[0].total || 0;
    const totalPages = Math.ceil(totalCount / limit);
    
    // Sinyalleri al
    const signalsQuery = `
      SELECT sr.*, wl.coin, wl.action, wl.timeframe
      FROM signal_results sr
      JOIN webhook_logs wl ON sr.webhook_log_id = wl.id 
      ${whereClause}
      ORDER BY ${orderBy} 
      LIMIT ?, ?
    `;
    
    console.log('Signals Query:', signalsQuery);
    
    const signals = await dbQuery(signalsQuery, [...queryParams, offset, limit]);
    console.log(`Found ${signals.length} signals`);
    
    // Tüm coinleri al (filtre için)
    const coinsResult = await dbQuery('SELECT DISTINCT coin FROM webhook_logs ORDER BY coin ASC');
    const coinsList = coinsResult.map(row => row.coin);
    
    // URL oluşturma yardımcı fonksiyonu
    const buildPageUrl = (pageNum) => {
      const url = new URL(req.protocol + '://' + req.get('host') + req.originalUrl);
      const params = new URLSearchParams(url.search);
      params.set('page', pageNum);
      return req.path + '?' + params.toString();
    };
    
    res.render('all-signals', {
      title: 'Tüm Sinyaller',
      signals,
      coins: coinsList,
      page,
      limit,
      totalCount,
      totalPages,
      sortOption,
      selectedFilters: {
        coin,
        action,
        status,
        dateRange
      },
      buildPageUrl,
      isSubscriber: subsinfo.isSubscriber
    });
  } catch (error) {
    console.error('All signals page error:', error);
    req.flash('error', 'Sinyaller yüklenirken bir hata oluştu: ' + error.message);
    res.redirect('/dashboard');
  }
});
  
  // Sinyal kapatma
  app.post('/signals/:id/close', requireLogin, async (req, res) => {
    try {
      const signalId = req.params.id;
      
      // Sinyali kapat (status'ü STOPPED olarak güncelle)
      await dbQuery(
        'UPDATE signal_results SET status = ?, completed_at = ? WHERE id = ?',
        ['STOPPED', new Date(), signalId]
      );
      
      req.flash('success', 'Sinyal başarıyla kapatıldı');
      res.redirect('/all-signals');
    } catch (error) {
      console.error('Signal closing error:', error);
      req.flash('error', 'Sinyal kapatılırken bir hata oluştu');
      res.redirect('/all-signals');
    }
  });

  // License activation
  app.get('/license', requireLogin, async (req, res) => {
    try {
      const userId = req.session.userId;
      const subsinfo = await checkIsUserSubscriber(userId);
      
      res.render('license', { 
        title: 'Lisans Aktivasyonu',
        isSubscriber: subsinfo.isSubscriber,
        endTime: subsinfo.endTime
      });
    } catch (error) {
      console.error('License page error:', error);
      req.flash('error', 'Lisans bilgisi yüklenirken bir hata oluştu');
      res.redirect('/dashboard');
    }
  });

  app.post('/license', requireLogin, async (req, res) => {
    try {
      const userId = req.session.userId;
      const { licenseKey } = req.body;
      
      // Lisans anahtarını kontrol et
      const licenseKeyInfo = await getLicenseKey(licenseKey);
      if (!licenseKeyInfo.status) {
        req.flash('error', licenseKeyInfo.message || 'Geçersiz lisans anahtarı');
        return res.redirect('/license');
      }
      
      // Lisans anahtarını kullan
      const useKeySt = await addDayToUser(licenseKeyInfo.day, userId);
      if (!useKeySt) {
        req.flash('error', 'Lisans anahtarı kullanılırken bir hata oluştu');
        return res.redirect('/license');
      }
      
      req.flash('success', `Lisans anahtarınız başarıyla etkinleştirildi. Yeni bitiş tarihiniz: ${useKeySt}`);
      res.redirect('/license');
    } catch (error) {
      console.error('License error:', error);
      req.flash('error', 'Lisans etkinleştirilirken bir hata oluştu');
      res.redirect('/license');
    }
  });
  
  // Bildirimler
  app.get('/notifications', requireLogin, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      const notifications = await dbQuery(
        'SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 50',
        [userId]
      );
      
      // Bildirimleri okundu olarak işaretle
      await dbQuery(
        'UPDATE notifications SET is_read = 1 WHERE user_id = ? AND is_read = 0',
        [userId]
      );
      
      res.render('notifications', { 
        title: 'Bildirimler',
        notifications 
      });
    } catch (error) {
      console.error('Notifications error:', error);
      req.flash('error', 'Bildirimler yüklenirken bir hata oluştu');
      res.redirect('/dashboard');
    }
  });

  // API endpoint - Okunmamış bildirim sayısı
  app.get('/api/notifications/unread-count', requireLogin, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      const result = await dbQuery(
        'SELECT COUNT(*) as count FROM notifications WHERE user_id = ? AND is_read = 0',
        [userId]
      );
      
      res.json({ count: result[0].count });
    } catch (error) {
      console.error('Unread notifications count error:', error);
      res.status(500).json({ error: 'Bildirim sayısı alınamadı' });
    }
  });

  // Trade route
  app.post('/trade', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      const { coin, action, leverage, amountInUSD, profitPercent1, profitPercent2, stopLossPercent } = req.body;
      
      // API anahtarları kontrolü
      const client = await getUserBinanceClient(userId);
      if (!client) {
        return res.status(400).json({ error: 'API anahtarları bulunamadı' });
      }
      
      // Fiyatı al
      const ticker = await client.futuresPrices({ symbol: coin });
      const currentPrice = parseFloat(ticker[coin]);
      
      // Hesaplamaları yap
      const calculated = calculateLeveragedValues(
        parseFloat(amountInUSD),
        parseFloat(leverage),
        parseFloat(stopLossPercent),
        parseFloat(profitPercent1),
        parseFloat(profitPercent2)
      );
      
      // İşlemi gerçekleştir
      const tradeResult = await executeTrade(client, userId, {
        coin,
        action,
        price: currentPrice,
        leverage: parseFloat(leverage),
        amountInUSD: parseFloat(amountInUSD),
        profitPercent1: parseFloat(profitPercent1),
        profitPercent2: parseFloat(profitPercent2),
        stopLossPercent: parseFloat(stopLossPercent),
        leveragedAmount: calculated.leveragedAmount,
        adjustedStopLoss: calculated.adjustedStopLoss,
        adjustedTakeProfit1: calculated.adjustedTakeProfit1,
        adjustedTakeProfit2: calculated.adjustedTakeProfit2
      });
      
      // Bildirim gönder
      await sendNotification(
        userId,
        `${coin} için ${action.toUpperCase()} emri başarıyla oluşturuldu.`,
        'success'
      );
      
      return res.json({ success: true, message: tradeResult });
    } catch (error) {
      console.error('Trade error:', error);
      return res.status(500).json({ error: error.message || 'İşlem gerçekleştirilirken bir hata oluştu' });
    }
  });

  // Balance route
  app.get('/balance', requireLogin, requireSubscription, async (req, res) => {
    try {
      const userId = req.session.userId;
      
      // API anahtarları kontrolü
      const client = await getUserBinanceClient(userId);
      if (!client) {
        return res.status(400).json({ error: 'API anahtarları bulunamadı' });
      }
      
      // Bakiye bilgisini al
      const accountInfo = await client.futuresAccountBalance();
      const walletBalance = accountInfo.find(b => b.asset === 'USDT').balance;
      const availableBalance = accountInfo.find(b => b.asset === 'USDT').availableBalance;
      
      // Günlük kar/zarar bilgisini al
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const todayTimestamp = today.getTime();
      
      const incomeHistory = await client.futuresIncome({
        startTime: todayTimestamp,
        incomeType: 'REALIZED_PNL'
      });
      
      const todayPNL = incomeHistory.reduce((total, income) => {
        return total + parseFloat(income.income);
      }, 0);
      
      // Açık pozisyon kar/zarar bilgisini al
      const positions = await client.futuresPositionRisk();
      const unrealizedPNL = positions.reduce((total, position) => {
        return total + parseFloat(position.unRealizedProfit);
      }, 0);
      
      return res.json({
        walletBalance: parseFloat(walletBalance),
        availableBalance: parseFloat(availableBalance),
        todayPNL,
        unrealizedPNL
      });
    } catch (error) {
      console.error('Balance error:', error);
      return res.status(500).json({ error: 'Bakiye bilgisi alınırken bir hata oluştu' });
    }
  });

  // Admin panel
  app.get('/admin', requireLogin, requireAdmin, async (req, res) => {
    try {
      // İstatistikler
      const stats = await dbQuery(`
        SELECT 
          (SELECT COUNT(*) FROM users) as total_users,
          (SELECT COUNT(*) FROM bultende_olanlar WHERE time > NOW()) as active_subscribers,
          (SELECT COUNT(*) FROM webhook_logs) as total_signals,
          (SELECT COUNT(*) FROM user_settings WHERE autotrade = 1) as autotrade_users
      `);
      
      // Son kullanıcılar
      const recentUsers = await dbQuery(`
        SELECT * FROM users 
        ORDER BY created_at DESC 
        LIMIT 5
      `);
      
      // Son sinyaller
      const recentSignals = await dbQuery(`
        SELECT * FROM webhook_logs 
        ORDER BY created_at DESC 
        LIMIT 5
      `);
      
      res.render('admin/dashboard', { 
        title: 'Admin Paneli',
        stats: stats[0],
        recentUsers,
        recentSignals
      });
    } catch (error) {
      console.error('Admin panel error:', error);
      req.flash('error', 'Admin paneli yüklenirken bir hata oluştu');
      res.render('admin/dashboard', { error: 'Veriler yüklenirken bir hata oluştu' });
    }
  });

  // Admin - Create User
  app.get('/admin/create-user', requireLogin, requireAdmin, (req, res) => {
    res.render('admin/create-user', { title: 'Kullanıcı Oluştur' });
  });

  app.post('/admin/create-user', requireLogin, requireAdmin, async (req, res) => {
    try {
      const { username, password, email, is_admin } = req.body;
      
      // Kullanıcı adı ve e-posta kontrolü
      const existing = await dbQuery(
        'SELECT * FROM users WHERE username = ? OR email = ?',
        [username, email]
      );
      
      if (existing.length > 0) {
        req.flash('error', 'Bu kullanıcı adı veya e-posta adresi zaten kullanılıyor');
        return res.redirect('/admin/create-user');
      }
      
      // Yeni kullanıcı oluştur
      const hashedPassword = hashPassword(password);
      const isAdmin = is_admin ? 1 : 0;
      
      await dbQuery(
        'INSERT INTO users (username, password, email, is_admin) VALUES (?, ?, ?, ?)',
        [username, hashedPassword, email, isAdmin]
      );
      
      req.flash('success', 'Kullanıcı başarıyla oluşturuldu');
      res.redirect('/admin/users');
    } catch (error) {
      console.error('Create user error:', error);
      req.flash('error', 'Kullanıcı oluşturulurken bir hata oluştu');
      res.redirect('/admin/create-user');
    }
  });

  // Admin - Users
  app.get('/admin/users', requireLogin, requireAdmin, async (req, res) => {
    try {
      const users = await dbQuery(`
        SELECT u.*, 
            (SELECT MAX(b.time) FROM bultende_olanlar b WHERE b.user_id = u.id) as subscription_end,
            (SELECT COUNT(*) FROM user_api_keys k WHERE k.user_id = u.id) as has_api_keys
        FROM users u
        ORDER BY u.created_at DESC
      `);
      
      res.render('admin/users', { 
        title: 'Kullanıcılar',
        users 
      });
    } catch (error) {
      console.error('Admin users error:', error);
      req.flash('error', 'Kullanıcılar listelenirken bir hata oluştu');
      res.redirect('/admin');
    }
  });

  // Admin - License key creation
  app.get('/admin/create-key', requireLogin, requireAdmin, (req, res) => {
    res.render('admin/create-key', { title: 'Lisans Anahtarı Oluştur' });
  });

  app.post('/admin/create-key', requireLogin, requireAdmin, async (req, res) => {
    try {
      const { days, count = 1 } = req.body;
      const parsedDays = parseInt(days);
      const parsedCount = parseInt(count);
      
      if (isNaN(parsedDays) || parsedDays <= 0) {
        req.flash('error', 'Geçersiz gün sayısı');
        return res.redirect('/admin/create-key');
      }
      
      if (isNaN(parsedCount) || parsedCount <= 0 || parsedCount > 50) {
        req.flash('error', 'Geçersiz anahtar sayısı (1-50 arası olmalı)');
        return res.redirect('/admin/create-key');
      }
      
      const keys = [];
      for (let i = 0; i < parsedCount; i++) {
        const key = [...Array(20)].map(() => Math.random().toString(36)[2]).join('');
        
        await dbQuery(
          'INSERT INTO license_keys (license_key, time, used) VALUES (?, ?, ?)', 
          [key, parsedDays, false]
        );
        
        keys.push({ key, days: parsedDays });
      }
      
      res.render('admin/keys-created', { 
        title: 'Oluşturulan Anahtarlar',
        keys 
      });
    } catch (error) {
      console.error('Create key error:', error);
      req.flash('error', 'Lisans anahtarı oluşturulurken bir hata oluştu');
      res.redirect('/admin/create-key');
    }
  });

  // Admin - List subscribers
  app.get('/admin/subscribers', requireLogin, requireAdmin, async (req, res) => {
    try {
      const subscribers = await dbQuery(`
        SELECT u.id, u.username, u.email, b.time as subscription_end
        FROM bultende_olanlar b
        JOIN users u ON b.user_id = u.id
        ORDER BY b.time DESC
      `);
      
      res.render('admin/subscribers', { 
        title: 'Aboneler',
        subscribers 
      });
    } catch (error) {
      console.error('Subscribers list error:', error);
      req.flash('error', 'Aboneler listelenirken bir hata oluştu');
      res.redirect('/admin');
    }
  });

  // Admin - License keys list
  app.get('/admin/license-keys', requireLogin, requireAdmin, async (req, res) => {
    try {
      const keys = await dbQuery(`
        SELECT * FROM license_keys
        ORDER BY used ASC, time DESC
      `);
      
      res.render('admin/license-keys', { 
        title: 'Lisans Anahtarları',
        keys 
      });
    } catch (error) {
      console.error('License keys list error:', error);
      req.flash('error', 'Lisans anahtarları listelenirken bir hata oluştu');
      res.redirect('/admin');
    }
  });

  // Admin - Send message to subscribers
  app.get('/admin/send-message', requireLogin, requireAdmin, (req, res) => {
    res.render('admin/send-message', { title: 'Mesaj Gönder' });
  });

  app.post('/admin/send-message', requireLogin, requireAdmin, async (req, res) => {
    try {
      const { message, messageType } = req.body;
      
      await broadcastNotification(message, messageType || 'info');
      
      req.flash('success', 'Mesaj tüm abonelere başarıyla gönderildi');
      res.redirect('/admin/send-message');
    } catch (error) {
      console.error('Send message error:', error);
      req.flash('error', 'Mesaj gönderilirken bir hata oluştu');
      res.redirect('/admin/send-message');
    }
  });

  // Error handling
  app.use((req, res, next) => {
    if (req.path.startsWith('/api/')) {
      next();
    } else {
      res.status(404).render('error', { 
        layout: false,
        title: 'Sayfa Bulunamadı',
        message: 'Aradığınız sayfa bulunamadı', 
        status: 404 
      });
    }
  });

  app.use((err, req, res, next) => {
    console.error(err);
    if (req.path.startsWith('/api/')) {
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      res.status(500).render('error', { 
        layout: false,
        title: 'Hata',
        message: 'Bir hata oluştu', 
        status: 500 
      });
    }
  });
}

// --------------- WEBHOOK KOD BAŞLANGICI ----------------

// JSON dosyası için dosya yolu
const signalsFilePath = path.join(__dirname, 'signals.json');
// Diğer const tanımlamalarının yanına ekleyin
const PRICE_CHECK_INTERVAL = 60000; // 60 saniye
// İzin verilen IP adresleri
const allowedIps = ['45.43.143.100', '45.43.143.201', '52.89.214.238', '217.131.110.230', '34.212.75.30', '54.218.53.128', '52.32.178.7', '213.142.149.161'];

// Geçerli timeframe değerleri
const validTimeframes = ['1', '3', '5', '15', '30', '60', '120', '240', '1D', '1W'];

async function calculateSuccessRate(action, coin, price) {
    try {
        // signals.json dosyasını oku
        const signalsData = await fs.readFile(signalsFilePath, 'utf8').catch(() => '[]');
        const signals = JSON.parse(signalsData);

        // Son 30 sinyali al
        const last30Signals = signals.slice(-30);
        
        // Coin'e göre filtrele
        const coinSignals = last30Signals.filter(signal => signal.coin === coin);
        
        if (coinSignals.length === 0) {
            return 75; // Varsayılan değer
        }

        // Başarı oranını hesapla
        let successCount = 0;
        
        for (const signal of coinSignals) {
            const signalPrice = parseFloat(signal.price);
            const currentPrice = parseFloat(price);
            
            if (signal.action.toLowerCase() === 'buy' || signal.action.toLowerCase() === 'long') {
                if (currentPrice > signalPrice) {
                    successCount++;
                }
            } else {
                if (currentPrice < signalPrice) {
                    successCount++;
                }
            }
        }

        const successRate = (successCount / coinSignals.length) * 100;
        
        // Minimum 51, maksimum 95 olacak şekilde sınırla
        return Math.min(Math.max(Math.round(successRate), 31), 95);

    } catch (error) {
        console.error('Error calculating success rate:', error);
        return 75; // Hata durumunda varsayılan değer
    }
}

async function updateSignalPrices() {
    try {
        // Aktif sinyalleri al
        const activeSignals = await dbQuery(`
            SELECT sr.*, wl.coin, wl.action 
            FROM signal_results sr 
            JOIN webhook_logs wl ON sr.webhook_log_id = wl.id 
            WHERE sr.status = 'ACTIVE'
        `);

        if (activeSignals.length === 0) return;

        // Tüm coinlerin fiyatlarını tek seferde al
        const coins = [...new Set(activeSignals.map(signal => signal.coin))];
        const prices = await Promise.all(coins.map(async (coin) => {
            try {
                // .P uzantısını kaldır ve Futures API kullan
                const cleanCoin = coin.replace('.P', '');
                const response = await axios.get(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${cleanCoin}`);
                return {
                    coin: coin,
                    price: parseFloat(response.data.price)
                };
            } catch (error) {
                console.error(`Error fetching price for ${coin}:`, error.message);
                return null;
            }
        }));

        // Her aktif sinyal için kontrol yap
        for (const signal of activeSignals) {
            const currentPrice = prices.find(p => p && p.coin === signal.coin)?.price;
            if (!currentPrice) continue;

            let newStatus = 'ACTIVE';
            let hitTp = null;
            let profitPercentage = null;

            // Long pozisyonlar için kontrol
            if (signal.action.toLowerCase() === 'buy' || signal.action.toLowerCase() === 'long') {
                // Stop loss kontrolü
                if (currentPrice <= signal.stop_loss) {
                    newStatus = 'STOPPED';
                    profitPercentage = ((signal.stop_loss - signal.entry_price) / signal.entry_price) * 100;
                }
                // TP kontrolleri
                else if (currentPrice >= signal.tp3_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 3;
                    profitPercentage = ((signal.tp3_price - signal.entry_price) / signal.entry_price) * 100;
                }
                else if (currentPrice >= signal.tp2_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 2;
                    profitPercentage = ((signal.tp2_price - signal.entry_price) / signal.entry_price) * 100;
                }
                else if (currentPrice >= signal.tp1_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 1;
                    profitPercentage = ((signal.tp1_price - signal.entry_price) / signal.entry_price) * 100;
                }
            }
            // Short pozisyonlar için kontrol
            else {
                // Stop loss kontrolü
                if (currentPrice >= signal.stop_loss) {
                    newStatus = 'STOPPED';
                    profitPercentage = ((signal.entry_price - signal.stop_loss) / signal.entry_price) * 100;
                }
                // TP kontrolleri
                else if (currentPrice <= signal.tp3_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 3;
                    profitPercentage = ((signal.entry_price - signal.tp3_price) / signal.entry_price) * 100;
                }
                else if (currentPrice <= signal.tp2_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 2;
                    profitPercentage = ((signal.entry_price - signal.tp2_price) / signal.entry_price) * 100;
                }
                else if (currentPrice <= signal.tp1_price) {
                    newStatus = 'COMPLETED';
                    hitTp = 1;
                    profitPercentage = ((signal.entry_price - signal.tp1_price) / signal.entry_price) * 100;
                }
            }

            // Durum değişmişse güncelle
            if (newStatus !== 'ACTIVE') {
                await dbQuery(`
                    UPDATE signal_results 
                    SET 
                        status = ?,
                        current_price = ?,
                        profit_percentage = ?,
                        tp_hit = ?,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                `, [
                    newStatus, 
                    currentPrice, 
                    profitPercentage, 
                    hitTp || 0,
                    signal.id
                ]);
            }
            // Aktif sinyallerin current_price'ını güncelle
            else {
                await dbQuery(`
                    UPDATE signal_results 
                    SET current_price = ?
                    WHERE id = ?
                `, [currentPrice, signal.id]);
            }
        }
    } catch (error) {
        console.error('Error in updateSignalPrices:', error);
    }
}

// Periyodik güncelleme başlat
setInterval(updateSignalPrices, PRICE_CHECK_INTERVAL);

// İlk çalıştırma
updateSignalPrices();

async function calculateManualAnalysis(action, coin, price, historical_prices) {
    const currentPrice = parseFloat(price);
    const prices = [
        parseFloat(historical_prices.price1),
        parseFloat(historical_prices.price2),
        parseFloat(historical_prices.price3),
        parseFloat(historical_prices.price4),
        parseFloat(historical_prices.price5)
    ].filter(p => !isNaN(p));

    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    
    let stopLoss, tp1, tp2, tp3;
    
    if (action.toLowerCase() === 'buy' || action.toLowerCase() === 'long') {
        stopLoss = minPrice * 0.995;
        const slDistance = currentPrice - stopLoss;
        
        tp1 = currentPrice + (slDistance * 2);
        tp2 = currentPrice + (slDistance * 3);
        tp3 = currentPrice + (slDistance * 4);
    } else {
        stopLoss = maxPrice * 1.005;
        const slDistance = stopLoss - currentPrice;
        
        tp1 = currentPrice - (slDistance * 2);
        tp2 = currentPrice - (slDistance * 3);
        tp3 = currentPrice - (slDistance * 4);
    }

    // Başarı oranını hesapla
    const aiRate = await calculateSuccessRate(action, coin, price);

    return {
        aiRate,
        stopLoss: parseFloat(stopLoss.toFixed(8)),
        targetPrices: {
            TP1: parseFloat(tp1.toFixed(8)),
            TP2: parseFloat(tp2.toFixed(8)),
            TP3: parseFloat(tp3.toFixed(8))
        }
    };
}

async function fetchAIAnalysis(action, coin, price, timeframe, ema, macd, macd_signal, rsi, historical_prices) {
    const useAI = process.env.AI_ANALYSIS_ENABLED === 'true';
    const aiService = process.env.AI_SERVICE?.toLowerCase() || 'openai';

    if (!useAI) {
        return calculateManualAnalysis(action, coin, price, historical_prices);
    }

    const prompt = `
    ### SYSTEM INSTRUCTION ###
    TradingView'den detaylı indikatör verisi geldi. Aşağıdaki bilgileri analiz et ve sinyalin tutma olasılığını belirle.
    Sen deneyimli bir kripto trading uzmanısın. Fiyat hareketini, teknik indikatörleri ve geçmiş fiyatları değerlendir.
    
    ### INPUT DATA ###
    İşlem Verileri:
    - Coin: ${coin}
    - İşlem Yönü: ${action}
    - Mevcut Fiyat: ${price}
    - Timeframe: ${timeframe}
    - Teknik Göstergeler:
      * EMA: ${ema}
      * MACD: ${macd}
      * MACD Signal: ${macd_signal}
      * RSI: ${rsi}
    - Son 5 Mum Kapanış (${timeframe} dakikalık):
      * P1: ${historical_prices.price1}
      * P2: ${historical_prices.price2}
      * P3: ${historical_prices.price3}
      * P4: ${historical_prices.price4}
      * P5: ${historical_prices.price5}
    
    ### ANALYSIS RULES ###
    1. AI Rate Hesaplama (Toplam 100 puan):
    
    RSI ANALİZİ (40 puan)
    BUY için:
    - RSI < 25: +40 puan
    - RSI 25-30: +35 puan
    - RSI 30-40: +25 puan
    - RSI 40-50: +15 puan
    - RSI 50-60: -10 puan
    - RSI 60-70: -25 puan
    - RSI > 70: -40 puan
    
    SELL için:
    - RSI > 75: +40 puan
    - RSI 70-75: +35 puan
    - RSI 60-70: +25 puan
    - RSI 50-60: +15 puan
    - RSI 40-50: -10 puan
    - RSI 30-40: -25 puan
    - RSI < 30: -40 puan
    
    MACD ANALİZİ (30 puan)
    BUY için:
    - MACD > Signal ve fark > %1: +30 puan
    - MACD > Signal ve fark %0.5-%1: +20 puan
    - MACD > Signal ve fark < %0.5: +10 puan
    - MACD < Signal ve fark < %0.5: -10 puan
    - MACD < Signal ve fark %0.5-%1: -20 puan
    - MACD < Signal ve fark > %1: -30 puan
    
    SELL için tersi uygulanır
    
    EMA ANALİZİ (15 puan)
    BUY için:
    - Fiyat > EMA ve fark > %1: +15 puan
    - Fiyat > EMA ve fark %0.5-%1: +10 puan
    - Fiyat > EMA ve fark < %0.5: +5 puan
    - Fiyat < EMA ve fark < %0.5: -5 puan
    - Fiyat < EMA ve fark %0.5-%1: -10 puan
    - Fiyat < EMA ve fark > %1: -15 puan
    
    SELL için tersi uygulanır
    
    TIMEFRAME ÇARPANI (Timeframe büyüdükçe güven artar ve ona göre çarpan verilir)
    - 1dk: x1
    - 5dk: x1.6
    - 15dk: x1.3
    - 30dk: x1.4
    - 1s: x1.5
    - 4s: x1.8
    - Günlük: x1.8
    
    TARİHSEL TREND (15 puan)
    BUY için:
    - Güçlü yükseliş (>%1): +15 puan
    - Yükseliş (%0.5-%1): +10 puan
    - Hafif yükseliş (<%0.5): +5 puan
    - Yatay: 0 puan
    - Hafif düşüş (<%0.5): -5 puan
    - Düşüş (%0.5-%1): -10 puan
    - Güçlü düşüş (>%1): -15 puan
    
    SELL için tersi uygulanır
    
    2. STOP LOSS HESAPLAMA
    - BUY için: Son 5 mumun en düşüğünün %0.5 altı
    - SELL için: Son 5 mumun en yükseğinin %0.5 üstü
    
    3. TAKE PROFIT HESAPLAMA
    - TP1: Giriş fiyatı ± (SL mesafesi * 2)
    - TP2: Giriş fiyatı ± (SL mesafesi * 3)
    - TP3: Giriş fiyatı ± (SL mesafesi * 4)
    (BUY için +, SELL için - kullanılır)
    
    ### RESPONSE FORMAT ###
    AI Rate: %X, Stop-Loss: X.XX, TP1: X.XX, TP2: X.XX, TP3: X.XX
    
    ### RULES ###
    0. Sadece yukarıdaki formatta yanıt ver
    1. AI Rate: 40-100 arası bir sayı olmalı
    2. Stop-Loss: ${action === 'buy' ? 'Giriş fiyatından düşük olmalı' : 'Giriş fiyatından yüksek olmalı'}
    3. TP1 < TP2 < TP3 ${action === 'buy' ? 've hepsi giriş fiyatından yüksek olmalı' : 've hepsi giriş fiyatından düşük olmalı'}
    4. Kesinlikle başka açıklama ekleme
    5. Tüm sayısal değerler noktadan sonra 2 basamak içermeli
    `;

    try {
        let result;

        if (aiService === 'gemini') {
            const response = await fetch(`https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key=${process.env.GOOGLE_API_KEY}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    contents: [{
                        role: "user",
                        parts: [{
                            text: prompt
                        }]
                    }],
                    generationConfig: {
                        temperature: 0.6,
                        maxOutputTokens: 150,
                        topP: 0.8,
                        topK: 10
                    },
                    safetySettings: [
                        {
                            category: "HARM_CATEGORY_HARASSMENT",
                            threshold: "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            category: "HARM_CATEGORY_HATE_SPEECH",
                            threshold: "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            category: "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                            threshold: "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            category: "HARM_CATEGORY_DANGEROUS_CONTENT",
                            threshold: "BLOCK_MEDIUM_AND_ABOVE"
                        }
                    ]
                })
            });
        
            if (!response.ok) {
                const errorBody = await response.text().catch(() => 'No error body');
                throw new Error(`Gemini API error: ${response.status}, Body: ${errorBody}`);
            }
        
            const data = await response.json();
            
            // Yanıt formatı değişti
            const completionText = data?.candidates?.[0]?.content?.parts?.[0]?.text?.trim();
            if (!completionText) {
                throw new Error('Gemini response is empty or invalid');
            }
        
            result = await processAIResponse(completionText, action, price);
        } else {
            const response = await axios.post('https://api.openai.com/v1/chat/completions', {
                model: 'gpt-4-turbo-preview',
                messages: [
                    {
                        role: 'system',
                        content: 'Sen deneyimli bir kripto trading uzmanısın. Teknik analiz konusunda uzmansın ve verilen tüm göstergeleri dikkatlice değerlendiriyorsun.'
                    },
                    {
                        role: 'user',
                        content: prompt
                    }
                ],
                max_tokens: 150,
                temperature: 0.6
            }, {
                headers: {
                    'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
                    'Content-Type': 'application/json'
                }
            });

            const completionText = response?.data?.choices?.[0]?.message?.content.trim();
            if (!completionText) {
                throw new Error('OpenAI response is empty or invalid');
            }

            result = await processAIResponse(completionText, action, price);
        }

        console.log(`${aiService.toUpperCase()} Analysis Result:`, result);
        return result;

    } catch (error) {
        console.error(`${aiService.toUpperCase()} Analysis Error:`, {
            message: error.message,
            response: error.response?.data,
            status: error.response?.status
        });
        
        // AI analizi başarısız olursa manuel analize geç
        console.log('Switching to manual analysis due to AI error');
        return calculateManualAnalysis(action, coin, price, historical_prices);
    }
}

async function processAIResponse(completionText, action, price) {
    const aiRateMatch = completionText.match(/AI Rate\s*:\s*%?(\d+)/);
    const stopLossMatch = completionText.match(/Stop-Loss\s*:\s*(\d+\.?\d*)/);
    const tpMatches = completionText.match(/TP1\s*:\s*(\d+\.?\d*),\s*TP2\s*:\s*(\d+\.?\d*),\s*TP3\s*:\s*(\d+\.?\d*)/);

    if (!aiRateMatch || !stopLossMatch || !tpMatches) {
        throw new Error('AI response format is not as expected');
    }

    const result = {
        aiRate: parseInt(aiRateMatch[1], 10),
        stopLoss: parseFloat(stopLossMatch[1]),
        targetPrices: {
            TP1: parseFloat(tpMatches[1]),
            TP2: parseFloat(tpMatches[2]),
            TP3: parseFloat(tpMatches[3])
        }
    };

    // Değer kontrolleri
    if (isNaN(result.aiRate) || result.aiRate < 1 || result.aiRate > 100) {
        throw new Error(`Invalid AI Rate: ${result.aiRate}`);
    }

    if (isNaN(result.stopLoss) || isNaN(result.targetPrices.TP1) || 
        isNaN(result.targetPrices.TP2) || isNaN(result.targetPrices.TP3)) {
        throw new Error('Invalid price values detected');
    }

    // Mantıksal kontroller
    const currentPrice = parseFloat(price);
    if (action.toLowerCase() === 'buy') {
        if (result.stopLoss >= currentPrice) {
            throw new Error('Stop-Loss must be below entry price for BUY');
        }
        if (result.targetPrices.TP1 <= currentPrice || 
            result.targetPrices.TP2 <= result.targetPrices.TP1 || 
            result.targetPrices.TP3 <= result.targetPrices.TP2) {
            throw new Error('Invalid target prices for BUY');
        }
    } else {
        if (result.stopLoss <= currentPrice) {
            throw new Error('Stop-Loss must be above entry price for SELL');
        }
        if (result.targetPrices.TP1 >= currentPrice || 
            result.targetPrices.TP2 >= result.targetPrices.TP1 || 
            result.targetPrices.TP3 >= result.targetPrices.TP2) {
            throw new Error('Invalid target prices for SELL');
        }
    }

    return result;
}

// IP kontrolü middleware
app.use('/api', (req, res, next) => {
    // IP kontrolü yapılmadan tüm isteklere izin ver
    next();
});


// Webhook veri doğrulama middleware
const validateWebhookData = (req, res, next) => {
    // İstek gövdesi kontrolü
    if (!req.body || Object.keys(req.body).length === 0) {
        console.error('Request body is empty or undefined:', {
            contentType: req.header('Content-Type'),
            contentLength: req.header('Content-Length'),
            method: req.method,
            url: req.url
        });
        return res.status(400).json({
            error: 'Request body is empty or could not be parsed',
            tip: 'Ensure you are sending JSON data with Content-Type: application/json header'
        });
    }
    
    console.log('Request body received:', JSON.stringify(req.body));
    
    const {
        action,
        coin,
        price,
        timeframe,
        ema,
        macd,
        macd_signal,
        rsi,
        historical_prices,
        size
    } = req.body;

    if (!action || !coin || !price || !timeframe) {
        return res.status(400).json({
            error: 'Missing required fields',
            required: ['action', 'coin', 'price', 'timeframe'],
            received: req.body
        });
    }

    if (!validTimeframes.includes(String(timeframe))) {
        return res.status(400).json({
            error: 'Invalid timeframe value',
            valid_values: validTimeframes,
            received: timeframe
        });
    }

    const normalizedAction = String(action).toLowerCase();
    if (!['buy', 'sell', 'long', 'short'].includes(normalizedAction)) {
        return res.status(400).json({
            error: 'Invalid action value',
            received: action
        });
    }

    req.validatedData = {
        action: normalizedAction,
        coin: coin.toUpperCase(),
        price: price,
        timeframe: timeframe,
        size: size || null,
        technicalIndicators: {
            ema: ema || '0',
            macd: macd || '0',
            macd_signal: macd_signal || '0',
            rsi: rsi || '0'
        },
        historical_prices: historical_prices || {
            price1: '0',
            price2: '0',
            price3: '0',
            price4: '0',
            price5: '0'
        },
        timestamp: new Date().toISOString()
    };

    next();
};

// WEBHOOK ROUTE
app.post('/api/webhook', validateWebhookData, async (req, res) => {
    try {
        const {
            action,
            coin,
            price,
            timeframe,
            size,
            technicalIndicators,
            historical_prices,
            timestamp
        } = req.validatedData;

        console.log('Processing webhook data:', { 
            action, 
            coin, 
            price, 
            timeframe,
            timestamp: new Date().toISOString()
        });

        let analysis;
        const useAI = process.env.AI_ANALYSIS_ENABLED === 'true';

        try {
            console.log(`Starting ${useAI ? 'AI' : 'manual'} analysis for ${coin}...`);
            
            if (useAI) {
                analysis = await fetchAIAnalysis(
                    action,
                    coin,
                    price,
                    timeframe,
                    technicalIndicators.ema,
                    technicalIndicators.macd,
                    technicalIndicators.macd_signal,
                    technicalIndicators.rsi,
                    historical_prices
                ).catch(error => {
                    console.error(`AI analysis failed for ${coin}:`, error);
                    console.log('Falling back to manual analysis...');
                    return calculateManualAnalysis(
                        action,
                        coin,
                        price,
                        historical_prices
                    );
                });
            } else {
                analysis = await calculateManualAnalysis(
                    action,
                    coin,
                    price,
                    historical_prices
                );
            }

            if (!analysis) {
                throw new Error('Analysis returned null or undefined');
            }

            console.log(`${useAI ? 'AI' : 'Manual'} Analysis Result for ${coin}:`, JSON.stringify(analysis));
            
            console.log(`Saving webhook log to database for ${coin}...`);
            const webhookLogResult = await dbQuery(
                'INSERT INTO webhook_logs (action, coin, price, timeframe, technical_indicators, historical_prices, ai_analysis) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [
                    action,
                    coin,
                    price,
                    timeframe,
                    JSON.stringify(technicalIndicators),
                    JSON.stringify(historical_prices),
                    JSON.stringify(analysis)
                ]
            ).catch(error => {
                console.error(`Failed to insert webhook log for ${coin}:`, error);
                throw new Error(`Database error: ${error.message}`);
            });

            // Webhook log ID'sini al
            const webhookLogId = webhookLogResult.insertId;
            console.log(`Webhook log saved with ID ${webhookLogId} for ${coin}`);

            // Sinyal sonuç takibi için kayıt ekle
            console.log(`Creating signal result for ${coin}...`);
            const signalResult = await dbQuery(
                'INSERT INTO signal_results (webhook_log_id, coin, action, entry_price, stop_loss, tp1_price, tp2_price, tp3_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                [
                    webhookLogId,
                    coin,
                    action,
                    price,
                    analysis.stopLoss,
                    analysis.targetPrices.TP1,
                    analysis.targetPrices.TP2,
                    analysis.targetPrices.TP3
                ]
            ).catch(error => {
                console.error(`Failed to insert signal result for ${coin}:`, error);
                throw new Error(`Database error: ${error.message}`);
            });
            
            console.log(`Signal result created with ID ${signalResult.insertId} for ${coin}`);

            // Sinyali kaydet
            console.log(`Saving signal to signals.json for ${coin}...`);
            await saveSignal(action, coin, price, timeframe, technicalIndicators, historical_prices)
                .catch(error => {
                    console.error(`Failed to save signal to file for ${coin}:`, error);
                    // Bu bir kritik hata değil, devam edebiliriz
                });
            
            if (analysis.aiRate < 40) { // veya istediğiniz başka bir değer
                console.log(`Signal for ${coin} skipped due to low AI rate: ${analysis.aiRate}`);
                return res.status(200).json({
                    status: 'skipped',
                    message: 'Signal skipped due to low AI rate',
                    aiRate: analysis.aiRate,
                    coin,
                    action,
                    price,
                    timeframe
                });
            }
            
            // Abonelere bildirim gönder
            console.log(`Sending notification to subscribers for ${coin}...`);
            await sendMessageToSubscribers(
                coin,
                action,
                price,
                timeframe,
                analysis,
                technicalIndicators,
                historical_prices
            ).catch(error => {
                console.error(`Failed to send message to subscribers for ${coin}:`, error);
                // Bu bir kritik hata değil, devam edebiliriz
            });

            console.log(`Webhook processing completed successfully for ${coin}`);
            res.status(200).json({
                status: 'success',
                message: `Signal processed successfully with ${useAI ? 'AI' : 'manual'} analysis`,
                coin,
                action,
                price,
                timeframe,
                analysis: {
                    aiRate: analysis.aiRate,
                    stopLoss: analysis.stopLoss,
                    targetPrices: analysis.targetPrices
                }
            });

        } catch (analysisError) {
            console.error(`Analysis error for ${coin || 'unknown coin'}:`, analysisError);
            console.error(`Analysis error stack:`, analysisError.stack);
            res.status(500).json({
                status: 'error',
                message: 'Analysis failed',
                error: analysisError.message,
                details: process.env.NODE_ENV === 'production' ? null : analysisError.stack
            });
        }

    } catch (error) {
        console.error('Webhook processing error:', error);
        console.error('Webhook error stack:', error.stack);
        console.error('Request body:', JSON.stringify(req.body, null, 2));
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message,
            details: process.env.NODE_ENV === 'production' ? null : error.stack
        });
    }
});

// Sabit admin şifresi - .env dosyasına eklenebilir
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'test';

// Basit session kontrolü için
const adminSessions = new Set();

// Admin Authentication Middleware
const authenticateAdmin = (req, res, next) => {
    const sessionToken = req.headers['admin-token'];
    if (!sessionToken || !adminSessions.has(sessionToken)) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
};

app.get('/api/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// Bulk License Creation Route
app.post('/api/admin/license/bulk-create', authenticateAdmin, async (req, res) => {
    try {
        const { count, duration } = req.body;
        
        // Validation
        if (!count || count < 1 || count > 100 || !duration || duration < 1) {
            return res.status(400).json({ 
                error: 'Invalid count or duration. Maximum 100 licenses can be created at once.' 
            });
        }

        const licenses = [];
        
        // Create multiple licenses
        for (let i = 0; i < count; i++) {
            const licenseKey = Math.random().toString(36).substring(2, 15).toUpperCase();
            
            await dbQuery(
                'INSERT INTO license_keys (license_key, time, used) VALUES (?, ?, ?)',
                [licenseKey, duration, 'false']
            );
            
            licenses.push(licenseKey);
        }
        
        res.json({ 
            success: true, 
            licenses,
            message: `${count} licenses created successfully`
        });
    } catch (error) {
        console.error('Bulk license creation error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/admin/webhook-logs', authenticateAdmin, async (req, res) => {
    try {
        const logs = await dbQuery(`
            SELECT * FROM webhook_logs 
            ORDER BY created_at DESC 
            LIMIT 100
        `);
        
        res.json(logs);
    } catch (error) {
        console.error('Webhook logs fetch error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Admin paneline yeni endpoint
app.get('/api/admin/signal-results', authenticateAdmin, async (req, res) => {
    try {
        const results = await dbQuery(`
            SELECT 
                sr.*,
                wl.coin,
                wl.action,
                wl.timeframe,
                wl.ai_analysis,
                wl.created_at as signal_created_at
            FROM signal_results sr
            JOIN webhook_logs wl ON sr.webhook_log_id = wl.id
            WHERE sr.status IN ('COMPLETED', 'STOPPED', 'ACTIVE')
            ORDER BY sr.created_at DESC
            LIMIT 100
        `);

        // Sayısal değerleri düzelt
        const formattedResults = results.map(result => ({
            ...result,
            profit_percentage: result.profit_percentage ? parseFloat(result.profit_percentage).toFixed(2) : null,
            entry_price: parseFloat(result.entry_price).toFixed(8),
            current_price: result.current_price ? parseFloat(result.current_price).toFixed(8) : null,
            stop_loss: parseFloat(result.stop_loss).toFixed(8),
            tp1_price: parseFloat(result.tp1_price).toFixed(8),
            tp2_price: parseFloat(result.tp2_price).toFixed(8),
            tp3_price: parseFloat(result.tp3_price).toFixed(8)
        }));

        res.json(formattedResults);
    } catch (error) {
        console.error('Signal results fetch error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Admin Login Route
app.post('/api/admin/login', (req, res) => {
    try {
        const { password } = req.body;
        
        if (password !== ADMIN_PASSWORD) {
            return res.status(401).json({ error: 'Invalid password' });
        }

        // Basit bir session token oluştur
        const sessionToken = Date.now().toString();
        adminSessions.add(sessionToken);
        
        res.json({ token: sessionToken });
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Admin Logout Route
app.post('/api/admin/logout', authenticateAdmin, (req, res) => {
    const sessionToken = req.headers['admin-token'];
    adminSessions.delete(sessionToken);
    res.json({ message: 'Logged out successfully' });
});

// License Key Management Routes
app.post('/api/admin/license/create', authenticateAdmin, async (req, res) => {
    try {
        const { duration } = req.body; // Duration in days
        const licenseKey = Math.random().toString(36).substring(2, 15).toUpperCase();
        
        await dbQuery(
            'INSERT INTO license_keys (license_key, time, used) VALUES (?, ?, ?)',
            [licenseKey, duration, 'false']
        );
        
        res.json({ licenseKey });
    } catch (error) {
        console.error('License creation error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/admin/licenses', authenticateAdmin, async (req, res) => {
    try {
        const licenses = await dbQuery('SELECT * FROM license_keys');
        res.json(licenses);
    } catch (error) {
        console.error('License fetch error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// User Management Routes
app.get('/api/admin/users', authenticateAdmin, async (req, res) => {
    try {
        const users = await dbQuery(`
            SELECT us.*, ua.api_key 
            FROM user_settings us 
            LEFT JOIN user_api_keys ua ON us.user_id = ua.user_id
        `);
        
        res.json(users);
    } catch (error) {
        console.error('User fetch error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/admin/reset-signal-results', authenticateAdmin, async (req, res) => {
    try {
        await dbQuery('TRUNCATE TABLE signal_results');
        
        res.json({
            success: true,
            message: 'Signal results have been reset successfully'
        });
    } catch (error) {
        console.error('Error resetting signal results:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to reset signal results'
        });
    }
});

app.delete('/api/admin/users/:userId', authenticateAdmin, async (req, res) => {
    try {
        const { userId } = req.params;
        
        await dbQuery('DELETE FROM user_settings WHERE user_id = ?', [userId]);
        await dbQuery('DELETE FROM user_api_keys WHERE user_id = ?', [userId]);
        
        res.json({ message: 'User deleted successfully' });
    } catch (error) {
        console.error('User deletion error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Trading Limits Management
app.put('/api/admin/trading-limits', authenticateAdmin, async (req, res) => {
    try {
        const { max_leverage, max_usdt } = req.body;
        
        await dbQuery(`
            UPDATE trading_limits 
            SET max_leverage = ?, max_usdt = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
        `, [max_leverage, max_usdt]);
        
        res.json({ message: 'Trading limits updated successfully' });
    } catch (error) {
        console.error('Trading limits update error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Dashboard Statistics
app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
    try {
        const [userCount] = await dbQuery('SELECT COUNT(*) as count FROM user_settings');
        const [licenseCount] = await dbQuery('SELECT COUNT(*) as count FROM license_keys WHERE used = "false"');
        const [activeTraders] = await dbQuery('SELECT COUNT(*) as count FROM user_settings WHERE autotrade = 1');
        
        res.json({
            totalUsers: userCount.count,
            availableLicenses: licenseCount.count,
            activeTraders: activeTraders.count
        });
    } catch (error) {
        console.error('Dashboard stats error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ------------------ WEBHOOK KOD SONU -------------------

async function startServer() {
  try {
    // İlk önce IP kontrolü yap


    // IP kontrolü başarılıysa devam et
    await handleDatabaseConnection();
    console.log('[TraderProBot] Veritabanı bağlantısı başarıyla kuruldu');
    
    // Veritabanı tablolarını kontrol et
    await ensureDatabaseTables();
    console.log('[TraderProBot] Veritabanı tabloları kontrol edildi');
    
    // Admin kullanıcısı var mı kontrol et, yoksa oluştur
    await ensureAdminUser();
    console.log('[TraderProBot] Admin kullanıcısı kontrol edildi');

    await startPolling();
    console.log('[TraderProBot] Bot başarıyla başlatıldı');

    // OrderTracker'ı initialize et
    console.log('[TraderProBot] Order takip sistemi başlatılıyor...');
    await orderTracker.validateAllOrders();
    console.log('[TraderProBot] Order takip sistemi başlatıldı');

    // Web dashboard app'i yapılandır
    console.log('[TraderProBot] Web arayüzü yapılandırılıyor...');
    configureWebDashboard();
    console.log('[TraderProBot] Web arayüzü yapılandırıldı');

    // Signals.json dosyasını oluştur eğer yoksa
    try {
      await fs.access(signalsFilePath);
    } catch (error) {
      await fs.writeFile(signalsFilePath, '[]');
      console.log('[TraderProBot] signals.json dosyası oluşturuldu');
    }

    // HTTP sunucusunu başlat
    server.listen(PORT, () => {
      console.log(`[TraderProBot] Server running on port ${PORT}`);
      console.log(`[TraderProBot] Dashboard URL: http://localhost:${PORT}`);
      console.log(`[TraderProBot] Webhook API accessible at http://localhost:${PORT}/api/webhook`);
    });

    console.log('[TraderProBot] Uygulama başarıyla başlatıldı');

    // Periyodik güvenlik kontrolü
    setInterval(async () => {
      const isStillSecure = await validateSecurity();
      if (!isStillSecure) {
        console.error('[TraderProBot] Security check failed. Shutting down...');
        await shutDown();
      }
    }, 3600000); // Her saat başı

    // Otomatik restart için zamanlayıcı
    setTimeout(async () => {
      console.log('[TraderProBot] Zamanlanmış yeniden başlatma başlatılıyor...');
      await shutDown();
    }, RESTART_INTERVAL);

  } catch (err) {
    console.error('[TraderProBot] Uygulama başlatılamadı:', err);
    process.exit(1);
  }
}

async function shutDown() {
  console.log('[TraderProBot] Uygulama kapatılıyor...');
  
  try {
    // OrderTracker'ı validate et
    console.log('[TraderProBot] Aktif emirler kontrol ediliyor...');
    await orderTracker.validateAllOrders();
    console.log('[TraderProBot] Emir kontrolü tamamlandı');

    // HTTP sunucusunu kapat
    await new Promise((resolve, reject) => {
      server.close((err) => {
        if (err) {
          console.error('[TraderProBot] HTTP sunucusu kapatılırken hata:', err);
          reject(err);
        } else {
          console.log('[TraderProBot] HTTP sunucusu kapatıldı');
          resolve();
        }
      });
    });
    
    const database = await handleDatabaseConnection();
    await database.end();
    console.log('[TraderProBot] Veritabanı bağlantısı kapatıldı');
    
    await stopPolling();
    console.log('[TraderProBot] Bot durduruldu');
    
    console.log('[TraderProBot] Uygulama başarıyla kapatıldı, yeniden başlatılıyor...');
    process.exit(0);
  } catch (error) {
    console.error('[TraderProBot] Kapatma işlemi sırasında hata:', error);
    process.exit(1);
  }
}

// Graceful shutdown handlers
process.on('SIGTERM', shutDown);
process.on('SIGINT', shutDown);

// Sunucuyu başlat
startServer();

module.exports = { app, server, sendMessageToSubscribers };