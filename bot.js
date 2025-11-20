require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const moment = require('moment');
const { dbQuery } = require('./db');
const { isJSONString, getLast10Signals, getDecimalPlaces, encrypt, decrypt, calculateLeveragedValues, checkTradingLimits } = require('./utils');
const { getMessage } = require('./lang');
const { testBinanceAPI, restartAllUserStreams, executeTrade, autoTradeUsers, startUserStream, stopUserStream, getUserNotificationStatus, updateUserNotificationStatus, closeAllPositionsAndOrders } = require('./binance_operations');
const { isAdmin, getLicenseKey, addDayToUser, checkIsUserSubscriber, getUserBinanceClient } = require('./user_management');
const Binance = require('binance-api-node').default;
const axios = require('axios');
const { ensureFreeUser, getFreeUsers, incrementAndShouldSend, canUseAnaliz, recordAnalizUse } = require('./free_tier');
const { performAnalysis } = require('./analysis');

const crypto = require('crypto');
const fs = require('fs');
const { showPositions, closePosition } = require('./positions');
const { validateSecurity } = require('./utils/security');
const _ = require('lodash');


const token = process.env.TELEGRAM_BOT_TOKEN;
const bot = new TelegramBot(token, { polling: true });
module.exports = { bot };

const userStates = {};
const userLocks = new Map();
const LOCK_TIMEOUT = 10000; // 60000'den 10000'e düşürüldü

async function acquireLock(chatId) {
    const existingLock = userLocks.get(chatId);
    if (existingLock) {
        if (Date.now() - existingLock.timestamp > LOCK_TIMEOUT) {
            releaseLock(chatId);
        } else {
            return false;
        }
    }

    userLocks.set(chatId, {
        timestamp: Date.now(),
        timeout: setTimeout(() => releaseLock(chatId), LOCK_TIMEOUT)
    });
    return true;
}

function releaseLock(chatId) {
    const lock = userLocks.get(chatId);
    if (lock && lock.timeout) {
        clearTimeout(lock.timeout);
    }
    userLocks.delete(chatId);
}

// State temizleme fonksiyonu
function cleanupStaleStates() {
    const now = Date.now();
    for (const [chatId, state] of Object.entries(userStates)) {
        if (now - state.lastUpdated > 30000) {
            delete userStates[chatId];
            releaseLock(chatId);
        }
    }
}

// Her dakika state temizliği yap
setInterval(cleanupStaleStates, 60000);

const settingMessages = {
  'amount_per_trade': 'enterTradeAmount',
  'profit_percent_1': 'enterProfitPercent1',
  'profit_percent_2': 'enterProfitPercent2',
  'stop_loss_percent': 'enterStopLoss',
  'leverage': 'enterLeverage'
};

(async () => {
  try {
    await restartAllUserStreams();
    console.log('All user streams restarted successfully');
  } catch (error) {
    console.error('Error restarting user streams:', error);
  }
})();

setInterval(async () => {
  try {
    const rows = await dbQuery('SELECT * FROM bultende_olanlar WHERE time <= NOW()');
    for (const user of rows) {
      try {
        await bot.sendMessage(user.id, getMessage('subscriptionExpired'));
        await stopUserStream(user.id);
        await dbQuery('UPDATE user_settings SET autotrade = 0 WHERE user_id = ?', [user.id]);
      } catch (error) {
        console.error(`Error processing expired subscription for user ${user.id}:`, error);
      }
    }
  } catch (error) {
    console.error('Error checking user subscriptions:', error);
  }
}, 24 * 60 * 60 * 1000);

setInterval(() => {
  const now = Date.now();
  for (const [chatId, state] of Object.entries(userStates)) {
    if (now - state.lastUpdated > 30 * 60 * 1000) {
      delete userStates[chatId];
    }
  }
}, 5 * 60 * 1000);




function getCancelKeyboard() {
  return {
    reply_markup: {
      inline_keyboard: [
        [{ text: 'İptal Et', callback_data: 'cancel_operation' }]
      ]
    }
  };
}

async function sendQuestionWithCancel(chatId, question) {
  return bot.sendMessage(chatId, question, getCancelKeyboard());
}


async function handleSettingResponse(chatId, msg) {
  console.log(`[${chatId}] Setting response received:`, msg.text);
  
  const userState = userStates[chatId];
  console.log(`[${chatId}] Current user state:`, userState);

  if (!userState || userState.stage !== 'completing_settings') {
      console.log(`[${chatId}] Invalid state or stage`);
      return;
  }

  if (userLocks.get(chatId)) {
      console.log(`[${chatId}] User is locked`);
      return;
  }

  userLocks.set(chatId, true);
  console.log(`[${chatId}] Lock acquired`);

  try {
      const value = parseFloat(msg.text);
      console.log(`[${chatId}] Parsed value:`, value);

      if (isNaN(value) || value < 0) {
          console.log(`[${chatId}] Invalid value`);
          await bot.sendMessage(chatId, 'Geçersiz değer. Lütfen pozitif bir sayı girin.');
          return;
      }

      const settings = ['amount_per_trade', 'leverage', 'profit_percent_1', 'profit_percent_2', 'stop_loss_percent'];
      const currentSetting = settings[userState.currentSettingIndex];
      console.log(`[${chatId}] Current setting:`, currentSetting);

      try {
          if (currentSetting === 'leverage') {
              console.log(`[${chatId}] Checking leverage limits`);
              await checkTradingLimits(value, 0);
          } else if (currentSetting === 'amount_per_trade') {
              console.log(`[${chatId}] Checking amount limits`);
              const userSettings = await dbQuery('SELECT leverage FROM user_settings WHERE user_id = ?', [chatId]);
              const leverage = userSettings.length > 0 ? userSettings[0].leverage : 1;
              await checkTradingLimits(leverage, value);
          }

          console.log(`[${chatId}] Saving setting to database`);
          await dbQuery(
              `INSERT INTO user_settings (user_id, ${currentSetting}) VALUES (?, ?) 
               ON DUPLICATE KEY UPDATE ${currentSetting} = ?`,
              [chatId, value, value]
          );

          userState.currentSettingIndex++;
          userState.lastUpdated = Date.now();
          console.log(`[${chatId}] Updated index:`, userState.currentSettingIndex);

          if (userState.currentSettingIndex >= settings.length) {
              console.log(`[${chatId}] All settings completed`);
              const updatedSettings = await dbQuery('SELECT * FROM user_settings WHERE user_id = ?', [chatId]);
              console.log(`[${chatId}] Retrieved settings:`, updatedSettings[0]);

              const { leverage, amount_per_trade, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type } = updatedSettings[0];

              const calculated = calculateLeveragedValues(
                  amount_per_trade,
                  leverage,
                  stop_loss_percent,
                  profit_percent_1,
                  profit_percent_2
              );
              console.log(`[${chatId}] Calculated values:`, calculated);

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
                      chatId
                  ]
              );

              const settingsSummary = getMessage('settingsSummary', {
                  amount: amount_per_trade,
                  leverage: leverage,
                  stopLoss: stop_loss_percent,
                  takeProfit1: profit_percent_1,
                  takeProfit2: profit_percent_2,
                  marginType: margin_type || 'isolated'
              });

              delete userStates[chatId];
              await bot.sendMessage(chatId, settingsSummary);
              await bot.sendMessage(chatId, getMessage('settingsCompleted'));
          } else {
              console.log(`[${chatId}] Asking next question`);
              const nextSettingMessages = {
                  'amount_per_trade': 'İşlem başına miktar (USDT) girin:',
                  'leverage': 'Kaldıraç oranını girin (1-125):',
                  'profit_percent_1': 'İlk kar alma yüzdesini girin:',
                  'profit_percent_2': 'İkinci kar alma yüzdesini girin:',
                  'stop_loss_percent': 'Zarar kesme yüzdesini girin:'
              };

              const nextSetting = settings[userState.currentSettingIndex];
              console.log(`[${chatId}] Next setting:`, nextSetting);
              await bot.sendMessage(chatId, nextSettingMessages[nextSetting], getCancelKeyboard());
          }
      } catch (error) {
          console.error(`[${chatId}] Error in settings:`, error);
          await bot.sendMessage(chatId, getMessage('errorSavingSetting'));
          delete userStates[chatId];
      }
  } finally {
      console.log(`[${chatId}] Lock released`);
      userLocks.delete(chatId);
  }
}

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text;
  
  console.log(`[${chatId}] Received message:`, text);
  console.log(`[${chatId}] Current userState:`, userStates[chatId]);

  if (!await acquireLock(chatId)) {
      return;
  }

  try {
      const userState = userStates[chatId];
      // Bu kısmı değiştiriyoruz
      if (userState && userState.stage === 'completing_settings') {
          console.log(`[${chatId}] Calling handleSettingResponse`);
          await handleSettingResponse(chatId, msg);
          return; // Bu return önemli
      }

      if (userState && Date.now() - userState.lastUpdated > 30000) {
          delete userStates[chatId];
          await bot.sendMessage(chatId, 'Oturum zaman aşımına uğradı. Lütfen işlemi tekrar başlatın.');
          return;
      }

      if (userState) {
          await handleUserState(chatId, text, userState, msg);
      } else {
          await handleNormalCommands(chatId, text);
      }
  } catch (error) {
      console.error(`[${chatId}] Error:`, error);
      await bot.sendMessage(chatId, getMessage('generalError'));
  } finally {
      releaseLock(chatId);
  }
});

async function handleUserState(chatId, text, userState, msg = null) {
  if (userState.stage === 'completing_settings') {
    await handleSettingResponse(chatId, { text });
  } else if (userState.waitingFor === 'license_key') {
    await handleLicenseKey(chatId, text);
  } else if (userState.waitingFor === 'admin_create_key_days') {
    await handleAdminCreateKey(chatId, text);
  } else if (userState.waitingFor === 'admin_give_admin_id') {
    await handleAdminGiveAdmin(chatId, text);
  } else if (userState.stage === 'ask_leverage') {
    await handleAskLeverage(chatId, text);
  } else if (userState.stage === 'ask_amount_usd') {
    await handleAskAmountUSD(chatId, text);
  } else if (userState.stage === 'ask_profit_percent_1') {
    await handleAskProfitPercent1(chatId, text);
  } else if (userState.stage === 'ask_profit_percent_2') {
    await handleAskProfitPercent2(chatId, text);
  } else if (userState.stage === 'ask_stop_loss') {
    await handleAskStopLoss(chatId, text);
  } else if (userState.stage === 'waiting_api_key') {
    const apiKey = text;
    userStates[chatId] = {
        stage: 'waiting_api_secret',
        apiKey: apiKey,
        lastUpdated: Date.now()
    };
    await sendQuestionWithCancel(chatId, getMessage('enterApiSecret'));
  } else if (userState.stage === 'waiting_api_secret') {
    const apiSecret = text;
    const apiKey = userState.apiKey;

    const isValid = await testBinanceAPI(apiKey, apiSecret);

    if (isValid) {
        const encryptedApiKey = encrypt(apiKey);
        const encryptedApiSecret = encrypt(apiSecret);

        try {
            await dbQuery('INSERT INTO user_api_keys (user_id, api_key, api_secret) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE api_key = VALUES(api_key), api_secret = VALUES(api_secret)', 
                [chatId, encryptedApiKey, encryptedApiSecret]);
            await bot.sendMessage(chatId, getMessage('apiKeysSaved'));
        } catch (error) {
            console.error(error);
            await bot.sendMessage(chatId, getMessage('apiKeysError'));
        }
    } else {
        await bot.sendMessage(chatId, getMessage('apiKeysInvalid'));
    }
    delete userStates[chatId];
  } else if (userState.stage === 'waiting_admin_message') {
    userStates[chatId] = {
        stage: 'waiting_admin_image',
        messageToSend: text,
        lastUpdated: Date.now()
    };
    await sendQuestionWithCancel(chatId, getMessage('imageToSubscribersPrompt'));
  } else if (userState.stage === 'waiting_admin_image') {
    const messageToSend = userState.messageToSend;
    let fileId = null;

    if (msg && msg.photo) {
        const photoSizes = msg.photo;
        const largestPhoto = photoSizes[photoSizes.length - 1];
        fileId = largestPhoto.file_id;
    } else if (msg && msg.document) {
        fileId = msg.document.file_id;
    }

    try {
        const result = await dbQuery("SELECT * FROM bultende_olanlar WHERE time > NOW()");
        let sentCount = 0;
        const totalCount = result.length;

        for (const subscriber of result) {
            try {
                if (fileId) {
                    await bot.sendPhoto(subscriber.id, fileId, { caption: messageToSend });
                } else {
                    await bot.sendMessage(subscriber.id, messageToSend);
                }
                sentCount++;
            } catch (error) {
                console.error(`Error sending message to user ${subscriber.id}:`, error);
            }
        }

        if (sentCount > 0) {
            await bot.sendMessage(chatId, getMessage('messageSentToSubscribers', { sentCount, totalCount }));
        } else {
            await bot.sendMessage(chatId, getMessage('noMessagesSent'));
        }
    } catch (error) {
        console.error(error);
        await bot.sendMessage(chatId, getMessage('errorOccurred'));
    }
    delete userStates[chatId];
  } else if (userState.stage === 'setting_max_leverage') {
    const leverage = parseInt(text);
    if (isNaN(leverage) || leverage <= 0 || leverage > 125) {
        await bot.sendMessage(chatId, 'Lütfen 1-125 arası bir değer girin.');
        return;
    }
    
    try {
        await dbQuery('UPDATE trading_limits SET max_leverage = ?', [leverage]);
        await bot.sendMessage(chatId, `Maksimum kaldıraç ${leverage}x olarak ayarlandı.`);
    } catch (error) {
        console.error(error);
        await bot.sendMessage(chatId, 'Bir hata oluştu.');
    }
    delete userStates[chatId];
  } else if (userState.stage === 'setting_max_usdt') {
    const usdt = parseFloat(text);
    if (isNaN(usdt) || usdt <= 0) {
        await bot.sendMessage(chatId, 'Lütfen geçerli bir USDT miktarı girin.');
        return;
    }
    
    try {
        await dbQuery('UPDATE trading_limits SET max_usdt = ?', [usdt]);
        await bot.sendMessage(chatId, `Maksimum USDT miktarı ${usdt} olarak ayarlandı.`);
    } catch (error) {
        console.error(error);
        await bot.sendMessage(chatId, 'Bir hata oluştu.');
    }
    delete userStates[chatId];
  }
}

async function handleNormalCommands(chatId, text) {
  if (text === '/start') {
    await handleStartCommand(chatId);
  
  } else if (text.startsWith('/analiz')) {
    await handleAnalizCommand(chatId, text);
} else if (text === '/admin') {
    await handleAdminCommand(chatId);
  } else if (text === '/buy') {
    await handleBuyCommand(chatId);
  } else if (text === '/support') {
    await handleSupportCommand(chatId);
  } else if (text === '/positions') {
    await handlePositionsCommand(chatId);
  } else if (text === '/binance') {
    await handleBinanceCommand(chatId);
  } else if (text === '/bildirim') {
    await handleBildirimCommand(chatId);
  } else if (text === '/balance') {
    await handleBalanceCommand(chatId);
  }
  
}

async function handleStartCommand(chatId) {
  try {
    const subsinfo = await checkIsUserSubscriber(chatId);
    // Telegram'da ilk kez başlayan kullanıcıya otomatik FREE üyelik tanımla (Web'de yok)
    if (!subsinfo.isSubscriber) {
      await ensureFreeUser(chatId);
    }

    
    // Kullanıcı bilgilerini Telegram'dan al
    const user = await bot.getChat(chatId);
    const username = user.username ? `@${user.username}` : user.first_name || 'Kullanıcı';

    const variables = {
      username: username,
      chatId: chatId,
      endTime: subsinfo.endTime || 'Mevcut değil'
    };
    
    let welcomeMessage = getMessage('welcomeMessage', variables);

    const replyOptions = {
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: subsinfo.isSubscriber ? getMessage('startReplySubscriber.text') : getMessage('startReplyNonSubscriber.text'),
              callback_data: subsinfo.isSubscriber ? getMessage('startReplySubscriber.callback_data') : getMessage('startReplyNonSubscriber.callback_data'),
            },
            {
              text: getMessage('prices'),
              callback_data: JSON.stringify({ type: "Prices" }),
            }
          ],
          [
            {
              text: getMessage('contactAdmin'),
              callback_data: JSON.stringify({ type: "support" }),
            },
            {
              text: getMessage('buyLicense'),
              url: 'https://t.me/Alexa_Destek'
            }
          ]
        ],
      },
    };

    await bot.sendMessage(chatId, welcomeMessage, replyOptions);
  } catch (error) {
    console.error('Error in /start command:', error);
    bot.sendMessage(chatId, 'Bir hata oluştu. Lütfen daha sonra tekrar deneyin.');
  }
}

async function handleAdminCommand(chatId) {
  const isAdminStt = await isAdmin(chatId);
  if (!isAdminStt) {
    return bot.sendMessage(chatId, getMessage('cannotPerformAction'));
  }

  const limits = await dbQuery('SELECT * FROM trading_limits ORDER BY id DESC LIMIT 1');
  const currentLimits = limits.length > 0 ? 
    `\n\nMevcut Limitler:\nMaksimum Kaldıraç: ${limits[0].max_leverage}x\nMaksimum USDT: ${limits[0].max_usdt} USDT` : '';

  bot.sendMessage(chatId, getMessage('welcomeAdmin') + currentLimits, {
    reply_markup: {
      inline_keyboard: [
        [
          { text: getMessage('createKey'), callback_data: JSON.stringify({ type: 'admin_create_key' }) },
          { text: getMessage('users'), callback_data: JSON.stringify({ type: 'admin_users' }) }
        ],
        [
          { text: getMessage('giveAdmin'), callback_data: JSON.stringify({ type: 'admin_give_admin' }) },
          { text: getMessage('sendMessages'), callback_data: JSON.stringify({ type: 'admin_send_messages' }) }
        ],
        [
          { text: '🔄 Maksimum Kaldıraç', callback_data: JSON.stringify({ type: 'admin_set_max_leverage' }) },
          { text: '💵 Maksimum USDT', callback_data: JSON.stringify({ type: 'admin_set_max_usdt' }) }
        ]
      ]
    }
  });
}

async function handleBuyCommand(chatId) {
  bot.sendMessage(chatId, getMessage('buyLicenseMessage'), {
    parse_mode: 'HTML',
    disable_web_page_preview: false
  });
}

async function handleSupportCommand(chatId) {
  bot.sendMessage(chatId, getMessage('supportMessage'), {
    parse_mode: 'HTML'
  });
}

async function handlePositionsCommand(chatId) {
  try {
    await showPositions(chatId, bot);
  } catch (error) {
    console.error(`Error showing positions for ${chatId}:`, error);
    bot.sendMessage(chatId, 'Pozisyonlar gösterilirken bir hata oluştu. Lütfen daha sonra tekrar deneyin.');
  }
}

async function handleBinanceCommand(chatId) {
  const subsinfo = await checkIsUserSubscriber(chatId);

  if (!subsinfo.isSubscriber) {
    return bot.sendMessage(chatId, getMessage('noLicense'));
  }

  const notificationStatus = await getUserNotificationStatus(chatId);
  const notificationText = notificationStatus ? getMessage('notificationsOff') : getMessage('notificationsOn');

  bot.sendMessage(chatId, getMessage('binanceOperations'), {
    reply_markup: {
      inline_keyboard: [
        [
          { text: getMessage('autoTradeToggle'), callback_data: JSON.stringify({ type: 'binance_autotrade' }) },
        ],
        [
          { text: getMessage('setApiKeys'), callback_data: JSON.stringify({ type: 'binance_setapi' }) },
          { text: getMessage('removeApiKeys'), callback_data: JSON.stringify({ type: 'binance_removeapi' }) },
        ],
        [
          { text: getMessage('tradeSettings'), callback_data: JSON.stringify({ type: 'binance_settings' }) },
          { text: getMessage('showBalance'), callback_data: JSON.stringify({ type: 'binance_balance' }) },
        ],
        [
          { text: getMessage('setMarginType'), callback_data: JSON.stringify({ type: 'binance_set_margin_type' }) },
        ],
        [
          { text: notificationText, callback_data: JSON.stringify({ type: 'binance_toggle_notifications' }) },
        ],
      ]
    }
  });
}

async function handleBildirimCommand(chatId) {
  try {
    await startUserStream(chatId);
    bot.sendMessage(chatId, 'Stop loss ve take profit bildirimleri başlatıldı.');
  } catch (error) {
    console.error(`Error starting user stream for ${chatId}:`, error);
    bot.sendMessage(chatId, 'Bildirimler başlatılırken bir hata oluştu. Lütfen daha sonra tekrar deneyin.');
  }
}

async function handleLicenseKey(chatId, key) {
  const LicenseKeyInfo = await getLicenseKey(key);
  if (LicenseKeyInfo.status) {
    const useKeySt = await addDayToUser(LicenseKeyInfo.day, chatId);
    if (useKeySt) {
      bot.sendMessage(chatId, getMessage('keyUsed', { endTime: useKeySt }));
    }
  } else {
    bot.sendMessage(chatId, getMessage('keyError', { message: LicenseKeyInfo.message }));
  }
  delete userStates[chatId];
}

async function handleAdminCreateKey(chatId, daysText) {
  const days = parseInt(daysText);
  if (isNaN(days)) {
    return bot.sendMessage(chatId, getMessage('invalidNumberOfDays'));
  }
  const key = [...Array(20)].map(() => Math.random().toString(36)[2]).join('');
  try {
    await dbQuery('INSERT INTO license_keys (license_key, time, used) VALUES (?, ?, ?)', [key, days, false]);
    bot.sendMessage(chatId, getMessage('keyCreatedWithDays', { key: key, days: days }));
  } catch (error) {
    console.error(error);
    bot.sendMessage(chatId, getMessage('errorOccurred'));
  }
  delete userStates[chatId];
}

async function handleAdminGiveAdmin(chatId, userId) {
  try {
    const results = await dbQuery('SELECT * FROM admins WHERE id = ?', [userId]);
    if (results.length > 0) {
      return bot.sendMessage(chatId, getMessage('alreadyAdmin'));
    }
    await dbQuery('INSERT INTO admins (id) VALUES (?)', [userId]);
    bot.sendMessage(chatId, getMessage('userIsNowAdmin', { userId }));
  } catch (error) {
    console.error(error);
    bot.sendMessage(chatId, getMessage('errorOccurred'));
  }
  delete userStates[chatId];
}

async function handleAskLeverage(chatId, text) {
  const leverage = parseInt(text);
  if (isNaN(leverage) || leverage <= 0) {
    return sendQuestionWithCancel(chatId, getMessage('invalidLeverage'));
  }

  try {
    await checkTradingLimits(leverage, 0);
  } catch (error) {
    return sendQuestionWithCancel(chatId, error.message);
  }

  userStates[chatId].tradeData.leverage = leverage;
  userStates[chatId].stage = 'ask_amount_usd';

  await sendQuestionWithCancel(chatId, getMessage('enterTradeAmount'));
}

async function handleAskAmountUSD(chatId, text) {
  const amountInUSD = parseFloat(text);
  if (isNaN(amountInUSD) || amountInUSD <= 0) {
    return sendQuestionWithCancel(chatId, getMessage('invalidAmount'));
  }

  try {
    await checkTradingLimits(userStates[chatId].tradeData.leverage, amountInUSD);
  } catch (error) {
    return sendQuestionWithCancel(chatId, error.message);
  }

  userStates[chatId].tradeData.amountInUSD = amountInUSD;
  userStates[chatId].stage = 'ask_profit_percent_1';

  await sendQuestionWithCancel(chatId, getMessage('enterProfitPercent1'));
}

async function handleAskProfitPercent1(chatId, text) {
  const profitPercent1 = parseFloat(text);
  if (isNaN(profitPercent1) || profitPercent1 < 0) {
    return sendQuestionWithCancel(chatId, getMessage('invalidProfitPercent'));
  }

  userStates[chatId].tradeData.profitPercent1 = profitPercent1;
  userStates[chatId].stage = 'ask_profit_percent_2';

  await sendQuestionWithCancel(chatId, getMessage('enterProfitPercent2'));
}

async function handleAskProfitPercent2(chatId, text) {
  const profitPercent2 = parseFloat(text);
  if (isNaN(profitPercent2) || profitPercent2 < 0) {
    return sendQuestionWithCancel(chatId, getMessage('invalidProfitPercent'));
  }

  userStates[chatId].tradeData.profitPercent2 = profitPercent2;
  userStates[chatId].stage = 'ask_stop_loss';

  await sendQuestionWithCancel(chatId, getMessage('enterStopLoss'));
}

async function handleAskStopLoss(chatId, text) {
  const stopLossPercent = parseFloat(text);
  if (isNaN(stopLossPercent) || stopLossPercent <= 0) {
    return sendQuestionWithCancel(chatId, getMessage('invalidStopLoss'));
  }

  userStates[chatId].tradeData.stopLossPercent = stopLossPercent;

  const { leveragedAmount, adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2 } = calculateLeveragedValues(
    userStates[chatId].tradeData.amountInUSD,
    userStates[chatId].tradeData.leverage,
    stopLossPercent,
    userStates[chatId].tradeData.profitPercent1,
    userStates[chatId].tradeData.profitPercent2
  );

  userStates[chatId].tradeData.leveragedAmount = leveragedAmount;
  userStates[chatId].tradeData.adjustedStopLoss = adjustedStopLoss;
  userStates[chatId].tradeData.adjustedTakeProfit1 = adjustedTakeProfit1;
  userStates[chatId].tradeData.adjustedTakeProfit2 = adjustedTakeProfit2;

  try {
    await dbQuery(
      'INSERT INTO user_settings (user_id, leverage, amount_per_trade, profit_percent_1, profit_percent_2, stop_loss_percent) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE leverage = VALUES(leverage), amount_per_trade = VALUES(amount_per_trade), profit_percent_1 = VALUES(profit_percent_1), profit_percent_2 = VALUES(profit_percent_2), stop_loss_percent = VALUES(stop_loss_percent)',
      [chatId, userStates[chatId].tradeData.leverage, userStates[chatId].tradeData.amountInUSD, userStates[chatId].tradeData.profitPercent1, userStates[chatId].tradeData.profitPercent2, stopLossPercent]
    );
  } catch (error) {
    console.error('Error saving user settings:', error);
    return bot.sendMessage(chatId, getMessage('errorSavingSettings'));
  }

  try {
    const client = Binance({
      apiKey: userStates[chatId].apiKey,
      apiSecret: userStates[chatId].apiSecret,
      futures: true
    });

    const ticker = await client.futuresPrices({ symbol: userStates[chatId].tradeData.coin });
    const currentPrice = parseFloat(ticker[userStates[chatId].tradeData.coin]);

    const { coin, action, leverage, amountInUSD, profitPercent1, profitPercent2, stopLossPercent, leveragedAmount, adjustedStopLoss, adjustedTakeProfit1, adjustedTakeProfit2 } = userStates[chatId].tradeData;
    const estimatedQuantity = leveragedAmount / currentPrice;
    const confirmationMessage = getMessage('tradeSummary', {
      coin: coin,
      action: action.toUpperCase(),
      price: currentPrice.toFixed(8),
      leverage: leverage,
      amount: amountInUSD,
      quantity: estimatedQuantity.toFixed(8),
      profit1: profitPercent1,
      profit2: profitPercent2,
      stopLoss: stopLossPercent,
      leveragedAmount: leveragedAmount.toFixed(2),
      adjustedStopLoss: adjustedStopLoss.toFixed(2),
      adjustedTakeProfit1: adjustedTakeProfit1.toFixed(2),
      adjustedTakeProfit2: adjustedTakeProfit2.toFixed(2)
    });

    await bot.sendMessage(chatId, confirmationMessage + '\n\n' + getMessage('confirmTrade'), {
      reply_markup: {
        inline_keyboard: [
          [
            { text: getMessage('confirm'), callback_data: 'confirm_trade' },
            { text: getMessage('cancel'), callback_data: 'cancel_trade' }
          ]
        ]
      }
    });
  } catch (error) {
    console.error('Error fetching price:', error);
    return bot.sendMessage(chatId, getMessage('priceError'));
  }
}

bot.on('callback_query', async (callbackQuery) => {
  const msg = callbackQuery.message;
  const chatId = msg.chat.id;
  let data;

  await bot.answerCallbackQuery(callbackQuery.id);

  if (userStates[chatId] && Date.now() - userStates[chatId].lastUpdated > 30000) {
      delete userStates[chatId];
      releaseLock(chatId);
      await bot.sendMessage(chatId, 'Oturum zaman aşımına uğradı. Lütfen tekrar deneyin.');
      return;
  }

  if (!await acquireLock(chatId)) {
      await bot.sendMessage(chatId, 'Önceki işleminiz devam ediyor, lütfen bekleyin.');
      return;
  }

  try {
      data = JSON.parse(callbackQuery.data);
  } catch (error) {
      data = callbackQuery.data;
  }

  try {
      if (data === 'cancel_operation') {
          delete userStates[chatId];
          await bot.sendMessage(chatId, 'İşlem iptal edildi.');
          return;
      }

      if (data === 'show_positions') {
          await showPositions(chatId, bot);
      } else if (typeof data === 'string' && data.startsWith('close_position_')) {
          const [_, __, symbol, side] = data.split('_');
          await closePosition(chatId, symbol, side, bot);
      } else if (typeof data === 'object' && data.type && data.type.startsWith('admin_')) {
          if (!await isAdmin(chatId)) {
              await bot.sendMessage(chatId, getMessage('cannotPerformAction'));
              return;
          }

          switch (data.type) {
              case 'admin_create_key':
                  userStates[chatId] = {
                      waitingFor: 'admin_create_key_days',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, getMessage('enterDaysForKey'), getCancelKeyboard());
                  break;

              case 'admin_users':
                  const results = await dbQuery('SELECT * FROM bultende_olanlar');
                  if (results.length === 0) {
                      await bot.sendMessage(chatId, getMessage('noSubscribers'));
                      return;
                  }
                  const subscribers = results.map((user, index) => 
                      `${index + 1}. ID: ${user.id}, Bitiş Tarihi: ${user.time}`).join('\n');
                  await bot.sendMessage(chatId, getMessage('subscribersList', { subscribers }));
                  break;

              case 'admin_give_admin':
                  userStates[chatId] = {
                      waitingFor: 'admin_give_admin_id',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, getMessage('enterUserId'), getCancelKeyboard());
                  break;

              case 'admin_send_messages':
                  userStates[chatId] = {
                      stage: 'waiting_admin_message',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, getMessage('messageToSubscribersPrompt'), getCancelKeyboard());
                  break;

              case 'admin_set_max_leverage':
                  userStates[chatId] = {
                      stage: 'setting_max_leverage',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, 'Lütfen maksimum kaldıraç miktarını girin (1-125):', getCancelKeyboard());
                  break;

              case 'admin_set_max_usdt':
                  userStates[chatId] = {
                      stage: 'setting_max_usdt',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, 'Lütfen maksimum USDT miktarını girin: \n(Hacim Olarak Girin! Önerilen Min:500)', getCancelKeyboard());
                  break;
          }
        } else if (typeof data === 'object' && data.type && data.type.startsWith('binance_')) {
          const subsinfo = await checkIsUserSubscriber(chatId);
          if (!subsinfo.isSubscriber) {
              await bot.sendMessage(chatId, getMessage('noLicense'));
              return;
          }
      
          switch (data.type) {
              case 'binance_settings':
                  userStates[chatId] = {
                      stage: 'completing_settings',
                      settingsToUpdate: ['amount_per_trade', 'leverage', 'profit_percent_1', 'profit_percent_2', 'stop_loss_percent'],
                      currentSettingIndex: 0,
                      lastUpdated: Date.now()
                  };
                  await askNextSetting(chatId);
                  break;

              case 'binance_autotrade':
                  const results = await dbQuery('SELECT autotrade FROM user_settings WHERE user_id = ?', [chatId]);
                  const autotrade = results.length > 0 ? !results[0].autotrade : true;
                  await dbQuery('INSERT INTO user_settings (user_id, autotrade) VALUES (?, ?) ON DUPLICATE KEY UPDATE autotrade = ?', 
                      [chatId, autotrade, autotrade]);
                  await bot.sendMessage(chatId, autotrade ? getMessage('autoTradeStarted') : getMessage('autoTradeStopped'));
                  break;

              case 'binance_setapi':
                  userStates[chatId] = {
                      stage: 'waiting_api_key',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, getMessage('enterApiKey'), getCancelKeyboard());
                  break;

              case 'binance_removeapi':
                  await dbQuery('DELETE FROM user_api_keys WHERE user_id = ?', [chatId]);
                  await bot.sendMessage(chatId, getMessage('apiKeysDeleted'));
                  break;

              case 'binance_balance':
                  const client = await getUserBinanceClient(chatId);
                  const accountInfo = await client.futuresAccountBalance();
                  const totalBalance = accountInfo.find(b => b.asset === 'USDT').balance;
                  await bot.sendMessage(chatId, getMessage('totalBalance', { 
                      balance: parseFloat(totalBalance).toFixed(2) 
                  }));
                  break;

              case 'binance_set_margin_type':
                  await bot.sendMessage(chatId, getMessage('chooseMarginType'), {
                      reply_markup: {
                          inline_keyboard: [
                              [
                                  { text: 'İzole', callback_data: JSON.stringify({ type: 'set_margin_type', value: 'isolated' }) },
                                  { text: 'Çapraz', callback_data: JSON.stringify({ type: 'set_margin_type', value: 'crossed' }) }
                              ]
                          ]
                      }
                  });
                  break;

              case 'binance_toggle_notifications':
                  const currentStatus = await getUserNotificationStatus(chatId);
                  const success = currentStatus ? 
                      await stopUserStream(chatId) : 
                      await startUserStream(chatId);

                  if (success) {
                      const newStatus = !currentStatus;
                      await bot.sendMessage(chatId, newStatus ? 'Bildirimler açıldı.' : 'Bildirimler kapatıldı.');
                      
                      const notificationText = newStatus ? '🔕 Bildirimleri Kapat' : '🔔 Bildirimleri Aç';
                      const updatedKeyboard = {
                          inline_keyboard: [
                              [{ text: getMessage('autoTradeToggle'), callback_data: JSON.stringify({ type: 'binance_autotrade' }) }],
                              [
                                  { text: getMessage('setApiKeys'), callback_data: JSON.stringify({ type: 'binance_setapi' }) },
                                  { text: getMessage('removeApiKeys'), callback_data: JSON.stringify({ type: 'binance_removeapi' }) }
                              ],
                              [
                                  { text: getMessage('tradeSettings'), callback_data: JSON.stringify({ type: 'binance_settings' }) },
                                  { text: getMessage('showBalance'), callback_data: JSON.stringify({ type: 'binance_balance' }) }
                              ],
                              [{ text: getMessage('setMarginType'), callback_data: JSON.stringify({ type: 'binance_set_margin_type' }) }],
                              [{ text: notificationText, callback_data: JSON.stringify({ type: 'binance_toggle_notifications' }) }]
                          ]
                      };
                      await bot.editMessageReplyMarkup(updatedKeyboard, {
                          chat_id: chatId,
                          message_id: msg.message_id
                      });
                  } else {
                      await bot.sendMessage(chatId, 'Bildirim durumu değiştirilirken bir hata oluştu.');
                  }
                  break;
          }
      } else if (data.type === 'set_margin_type') {
          const client = await getUserBinanceClient(chatId);
          if (!client) {
              throw new Error('API anahtarları bulunamadı');
          }
          const result = await changeMarginType(client, chatId, data.value);
          if (result.success) {
              await bot.sendMessage(chatId, result.message);
          } else {
              throw new Error(result.message);
          }
      } else if (typeof data === 'string' && data.startsWith('ep_')) {
          const [_, coin, action, price] = data.split('_');
          const actualPrice = price.replace(/_/g, '.');
          
          const subsinfo = await checkIsUserSubscriber(chatId);
          if (!subsinfo.isSubscriber) {
              await bot.sendMessage(chatId, getMessage('noLicense'));
              return;
          }

          const results = await dbQuery('SELECT api_key, api_secret FROM user_api_keys WHERE user_id = ?', [chatId]);
          if (results.length === 0) {
              await bot.sendMessage(chatId, getMessage('apiKeysNotFound'));
              return;
          }

          userStates[chatId] = {
              stage: 'ask_leverage',
              tradeData: {
                  coin,
                  action,
                  price: actualPrice
              },
              apiKey: decrypt(results[0].api_key),
              apiSecret: decrypt(results[0].api_secret),
              lastUpdated: Date.now()
          };

          await bot.sendMessage(chatId, getMessage('enterLeverage'), getCancelKeyboard());
      } else if (data === 'confirm_trade' || data === 'cancel_trade') {
          if (data === 'confirm_trade') {
              const userState = userStates[chatId];
              if (!userState || !userState.apiKey || !userState.apiSecret) {
                  await bot.sendMessage(chatId, getMessage('apiKeysNotFound'));
                  return;
              }

              const client = Binance({
                  apiKey: userState.apiKey,
                  apiSecret: userState.apiSecret,
                  futures: true
              });

              try {
                  const tradeMessage = await executeTrade(client, chatId, {
                      ...userState.tradeData,
                      leveragedAmount: userState.tradeData.leveragedAmount,
                      adjustedStopLoss: userState.tradeData.adjustedStopLoss,
                      adjustedTakeProfit1: userState.tradeData.adjustedTakeProfit1,
                      adjustedTakeProfit2: userState.tradeData.adjustedTakeProfit2,
                      marginType: userState.tradeData.marginType
                  });
                  await bot.sendMessage(chatId, tradeMessage);
              } catch (error) {
                  console.error('Error executing trade:', error);
                  await bot.sendMessage(chatId, getMessage('tradeExecutionError', { error: error.message }));
              }
          } else {
              await bot.sendMessage(chatId, getMessage('tradeCancelled'));
          }
          delete userStates[chatId];
      } else if (typeof data === 'object' && data.type) {
          switch (data.type) {
              case "Subscribe":
                  userStates[chatId] = { 
                      waitingFor: 'license_key',
                      lastUpdated: Date.now()
                  };
                  await bot.sendMessage(chatId, getMessage('keyPrompt'), getCancelKeyboard());
                  break;

              case "support":
                  await bot.sendMessage(chatId, getMessage('adminAccount'));
                  break;

              case "Prices":
                  await bot.sendMessage(chatId, getMessage('pricesMessage'), {
                      reply_markup: {
                          inline_keyboard: [[{
                              text: getMessage('buyLicenseButton'),
                              url: 'https://t.me/emirwebtasarim'
                          }]]
                      }
                  });
                  break;

              default:
                  await bot.sendMessage(chatId, getMessage('unknownCommand'));
          }
      } else {
          await bot.sendMessage(chatId, getMessage('unknownCommand'));
      }
  } catch (error) {
      console.error(`Error for user ${chatId}:`, error);
      await bot.sendMessage(chatId, getMessage('generalError'));
  } finally {
      releaseLock(chatId);
  }
});

async function handleSettingResponse(chatId, msg) {
  console.log(`[${chatId}] handleSettingResponse started`);
  
  const userState = userStates[chatId];
  console.log(`[${chatId}] Current user state in handler:`, userState);

  if (!userState || userState.stage !== 'completing_settings') {
      console.log(`[${chatId}] Invalid state or stage, exiting handler`);
      return;
  }

  const value = parseFloat(msg.text);
  console.log(`[${chatId}] Parsed value:`, value);

  if (isNaN(value) || value < 0) {
      console.log(`[${chatId}] Invalid value`);
      await bot.sendMessage(chatId, 'Geçersiz değer. Lütfen pozitif bir sayı girin.');
      return;
  }

  const settings = ['amount_per_trade', 'leverage', 'profit_percent_1', 'profit_percent_2', 'stop_loss_percent'];
  const currentSetting = settings[userState.currentSettingIndex];
  console.log(`[${chatId}] Current setting:`, currentSetting);

  try {
      if (currentSetting === 'leverage') {
          console.log(`[${chatId}] Checking leverage limits`);
          await checkTradingLimits(value, 0);
      } else if (currentSetting === 'amount_per_trade') {
          console.log(`[${chatId}] Checking amount limits`);
          const userSettings = await dbQuery('SELECT leverage FROM user_settings WHERE user_id = ?', [chatId]);
          const leverage = userSettings.length > 0 ? userSettings[0].leverage : 1;
          await checkTradingLimits(leverage, value);
      }

      console.log(`[${chatId}] Saving setting to database`);
      await dbQuery(
          `INSERT INTO user_settings (user_id, ${currentSetting}) VALUES (?, ?) 
           ON DUPLICATE KEY UPDATE ${currentSetting} = ?`,
          [chatId, value, value]
      );

      userState.currentSettingIndex++;
      userState.lastUpdated = Date.now();
      console.log(`[${chatId}] Updated index:`, userState.currentSettingIndex);

      if (userState.currentSettingIndex >= settings.length) {
          console.log(`[${chatId}] All settings completed`);
          const updatedSettings = await dbQuery('SELECT * FROM user_settings WHERE user_id = ?', [chatId]);
          console.log(`[${chatId}] Retrieved settings:`, updatedSettings[0]);

          const { leverage, amount_per_trade, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type } = updatedSettings[0];

          const calculated = calculateLeveragedValues(
              amount_per_trade,
              leverage,
              stop_loss_percent,
              profit_percent_1,
              profit_percent_2
          );
          console.log(`[${chatId}] Calculated values:`, calculated);

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
                  chatId
              ]
          );

          const settingsSummary = getMessage('settingsSummary', {
              amount: amount_per_trade,
              leverage: leverage,
              stopLoss: stop_loss_percent,
              takeProfit1: profit_percent_1,
              takeProfit2: profit_percent_2,
              marginType: margin_type || 'isolated'
          });

          delete userStates[chatId];
          await bot.sendMessage(chatId, settingsSummary);
          await bot.sendMessage(chatId, getMessage('settingsCompleted'));
      } else {
          console.log(`[${chatId}] Asking next question`);
          const nextSettingMessages = {
            'amount_per_trade': '💰 İşlem Başına USDT Miktarını Girin:\n\n📌 Örnek: 100 USDT',
            'leverage': '📊 Kaldıraç Oranını Girin (1-125):\n\n📌 Örnek: 20x kaldıraç için sadece 20 yazın',
            'profit_percent_1': '📈 1. Take-Profit Yüzdesini Girin:\n\n📌 Örnek: %2 kar için sadece 2 yazın',
            'profit_percent_2': '📈 2. Take-Profit Yüzdesini Girin:\n⚠️Kullanmak istemiyorsanız 0 yazın.\n\n📌 Örnek: %5 kar için sadece 5 yazın',
            'stop_loss_percent': '🔴 Stop-Loss Yüzdesini Girin:\n\n📌 Örnek: %2 stop için sadece 2 yazın\n⚠️ Kaldıraç arttıkça stop yüzdesini düşük tutmanız önerilir'
        };

          const nextSetting = settings[userState.currentSettingIndex];
          console.log(`[${chatId}] Next setting:`, nextSetting);
          await bot.sendMessage(chatId, nextSettingMessages[nextSetting], getCancelKeyboard());
      }
  } catch (error) {
      console.error(`[${chatId}] Error in settings:`, error);
      await bot.sendMessage(chatId, getMessage('errorSavingSetting'));
      delete userStates[chatId];
  }
}

async function saveAndUpdateSettings(chatId, setting, value, userState) {
  try {
      await dbQuery(
          `INSERT INTO user_settings (user_id, ${setting}) VALUES (?, ?) 
           ON DUPLICATE KEY UPDATE ${setting} = ?`,
          [chatId, value, value]
      );

      userState.currentSettingIndex++;
      userState.lastUpdated = Date.now();

      if (userState.currentSettingIndex >= userState.settingsToUpdate.length) {
          await finalizeSettings(chatId);
      } else {
          await askNextSetting(chatId);
      }
  } catch (error) {
      console.error('Error saving settings:', error);
      throw error;
  }
}


async function askForSettings(chatId) {
  userStates[chatId] = {
    stage: 'completing_settings',
    settingsToUpdate: Object.keys(settingMessages),
    currentSettingIndex: 0,
    lastUpdated: Date.now()
  };

  await askNextSetting(chatId);
}

async function finalizeSettings(chatId) {
  try {
      const settings = await dbQuery('SELECT * FROM user_settings WHERE user_id = ?', [chatId]);
      if (settings.length === 0) {
          throw new Error('Settings not found');
      }

      const { leverage, amount_per_trade, profit_percent_1, profit_percent_2, stop_loss_percent, margin_type } = settings[0];

      const calculated = calculateLeveragedValues(
          amount_per_trade,
          leverage,
          stop_loss_percent,
          profit_percent_1,
          profit_percent_2
      );

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
              chatId
          ]
      );
      
      const summary = getMessage('settingsSummary', {
          amount: amount_per_trade,
          leverage,
          stopLoss: stop_loss_percent,
          takeProfit1: profit_percent_1,
          takeProfit2: profit_percent_2,
          marginType: margin_type
      });

      delete userStates[chatId];
      await bot.sendMessage(chatId, summary);
      await bot.sendMessage(chatId, getMessage('settingsCompleted'));
  } catch (error) {
      console.error('Error finalizing settings:', error);
      throw error;
  }
}

process.on('unhandledRejection', (error) => {
  console.error('Unhandled promise rejection:', error);
});

setInterval(async () => {
  try {
      const now = Date.now();
      for (const [chatId, lock] of userLocks.entries()) {
          if (now - lock.timestamp > LOCK_TIMEOUT) {
              releaseLock(chatId);
          }
      }
  } catch (error) {
      console.error('Error in cleanup interval:', error);
  }
}, LOCK_TIMEOUT / 2);

async function askNextSetting(chatId) {
  const userState = userStates[chatId];
  if (!userState || !userState.settingsToUpdate) {
      return;
  }

  if (userState.currentSettingIndex < userState.settingsToUpdate.length) {
      const currentSetting = userState.settingsToUpdate[userState.currentSettingIndex];
      await sendQuestionWithCancel(chatId, getMessage(settingMessages[currentSetting]));
  }
}

async function changeMarginType(client, chatId, newMarginType) {
  try {
    await dbQuery('UPDATE user_settings SET margin_type = ? WHERE user_id = ?', [newMarginType, chatId]);

    return {
      success: true,
      message: getMessage('marginTypeSet', { type: newMarginType.toUpperCase() }),
    };
  } catch (error) {
    console.error('Error changing margin type:', error);
    return {
      success: false,
      message: getMessage('errorChangingMarginType') + '\n' + error.message,
    };
  }
}



async function sendMessageToSubscribers(coin, action, price, timeframe, targetPrices, technicalIndicators, historical_prices) {
  try {
      // Gelen parametreleri kontrol et
      if (!coin || !action || !price || !targetPrices) {
          console.error('Missing required parameters:', { coin, action, price, targetPrices });
          return;
      }

      // Action'ı string'e çevir ve küçük harfe dönüştür
      const normalizedAction = String(action).toLowerCase();

      // Son sinyalleri al ve trend analizi yap
      const signals = await getLast10Signals();
      const totalWeight = signals.length * (signals.length + 1) / 2;
      let weightedBuySignals = 0;
      let weightedSellSignals = 0;

      signals.forEach((signal, index) => {
          const weight = index + 1;
          const signalAction = String(signal.action || '').toLowerCase();

          if (signalAction === 'buy' || signalAction === 'long') {
              weightedBuySignals += weight;
          } else if (signalAction === 'sell' || signalAction === 'short') {
              weightedSellSignals += weight;
          }
      });

      const buyPercentage = ((weightedBuySignals / totalWeight) * 100).toFixed(2);
      const sellPercentage = ((weightedSellSignals / totalWeight) * 100).toFixed(2);

      // Son 3 sinyale göre trend belirle
      const recentTrend = signals.slice(-3).map(s => String(s.action || '').toLowerCase());
      let trendMessage = '';
      
      if (recentTrend.every(a => a === 'buy' || a === 'long')) {
          trendMessage = getMessage('trendUp');
      } else if (recentTrend.every(a => a === 'sell' || a === 'short')) {
          trendMessage = getMessage('trendDown');
      } else {
          trendMessage = getMessage('trendNeutral');
      }

      // Fiyat geçmişine göre ek trend analizi
      const priceHistory = historical_prices ? [
          historical_prices.price5,
          historical_prices.price4,
          historical_prices.price3,
          historical_prices.price2,
          historical_prices.price1,
          price
      ].map(Number) : [];

      if (priceHistory.length > 0) {
          const isConsistentUptrend = priceHistory.every((val, i, arr) => i === 0 || val >= arr[i - 1]);
          const isConsistentDowntrend = priceHistory.every((val, i, arr) => i === 0 || val <= arr[i - 1]);

          if (isConsistentUptrend) {
              trendMessage = getMessage('trendUp');
          } else if (isConsistentDowntrend) {
              trendMessage = getMessage('trendDown');
          }
      }

      // Technical indicators mesajını oluştur
      const technicalInfo = technicalIndicators ? 
          `\nRSI: ${technicalIndicators.rsi}\nMACD: ${technicalIndicators.macd}\nEMA: ${technicalIndicators.ema}` : '';

      const text = getMessage('newAlarm', {
          coin,
          timeframe: timeframe || '--',
          price,
          action: normalizedAction.toUpperCase(),
          aiRate: `%${targetPrices.aiRate || '--'}`,
          tp1: targetPrices.targetPrices?.TP1 || '--',
          tp2: targetPrices.targetPrices?.TP2 || '--',
          tp3: targetPrices.targetPrices?.TP3 || '--',
          stopLoss: targetPrices.stopLoss || '--',
          rsi: technicalIndicators.rsi || '--',
          macd: technicalIndicators.macd || '--',
          ema: technicalIndicators.ema || '--',
          trendMessage
      });

      const callbackData = `ep_${coin}_${normalizedAction}_${price}`.replace(/\./g, '_');

      const inlineKeyboard = {
          reply_markup: {
              inline_keyboard: [
                  [
                      {
                          text: '📈 Binance\'de Pozisyona Gir',
                          callback_data: callbackData
                      }
                  ],
                  [
                      {
                          text: '📊 Aktif Pozisyonlarım',
                          callback_data: 'show_positions'
                      }
                  ]
              ]
          }
      };

      const subscribers = await dbQuery("SELECT * FROM bultende_olanlar WHERE time > NOW()");

      // FREE TIER: Her 20 sinyalde bir, free kullanıcılara da gönder
      const sendToFree = await incrementAndShouldSend();
      let freeUsers = [];
      if (sendToFree) {
          freeUsers = await getFreeUsers();
      }


      for (const subscriber of subscribers) {
          try {
              await bot.sendMessage(subscriber.id, text, inlineKeyboard);
          } catch (error) {
              console.error(`Error sending message to user ${subscriber.id}:`, error);
          }
      }
      // Send to FREE users if applicable
      if (sendToFree && freeUsers.length > 0) {
          for (const uid of freeUsers) {
              try {
                  await bot.sendMessage(uid, text, inlineKeyboard);
              } catch (error) {
                  console.error(`Error sending free message to user ${uid}:`, error);
              }
          }
      }


      // Auto-trade işlemini gerçekleştir
      try {
          await autoTradeUsers(coin, normalizedAction, price, targetPrices);
      } catch (error) {
          console.error('Error in autoTradeUsers:', error);
      }
  } catch (err) {
      console.error('Error in sendMessageToSubscribers:', err);
  }
}



function startPolling() {
  return new Promise((resolve) => {
    console.log('[TraderProBot] Bot polling zaten aktif');
    resolve();
  });
}

function stopPolling() {
  return new Promise((resolve) => {
    bot.stopPolling()
      .then(() => {
        console.log('[TraderProBot] Bot polling durduruldu');
        resolve();
      })
      .catch((error) => {
        console.error('[TraderProBot] Bot polling durdurulurken hata oluştu:', error);
        resolve();
      });
  });
}

async function handleBalanceCommand(chatId) {
  try {
      const subsinfo = await checkIsUserSubscriber(chatId);
      if (!subsinfo.isSubscriber) {
          return bot.sendMessage(chatId, getMessage('noLicense'));
      }

      const client = await getUserBinanceClient(chatId);
      if (!client) {
          return bot.sendMessage(chatId, getMessage('apiKeysNotFound'));
      }

      const accountInfo = await client.futuresAccountBalance();
      const walletBalance = accountInfo.find(b => b.asset === 'USDT').balance;
      const availableBalance = accountInfo.find(b => b.asset === 'USDT').availableBalance;

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

      const positions = await client.futuresPositionRisk();
      const unrealizedPNL = positions.reduce((total, position) => {
          return total + parseFloat(position.unRealizedProfit);
      }, 0);

      const todayPNLEmoji = todayPNL > 0 ? '🟢' : todayPNL < 0 ? '🔴' : '⚪';
      const unrealizedPNLEmoji = unrealizedPNL > 0 ? '🟢' : unrealizedPNL < 0 ? '🔴' : '⚪';

      const balanceMessage = `💼 HESAP BİLGİLERİ\n\n` +
          `👛 Toplam Bakiye: ${parseFloat(walletBalance).toFixed(2)} USDT\n` +
          `💵 Kullanılabilir Bakiye: ${parseFloat(availableBalance).toFixed(2)} USDT\n` +
          `${todayPNLEmoji} Bugünkü Kar/Zarar: ${todayPNL.toFixed(2)} USDT\n` +
          `${unrealizedPNLEmoji} Açık Pozisyon Kar/Zarar: ${unrealizedPNL.toFixed(2)} USDT`;

      await bot.sendMessage(chatId, balanceMessage);
  } catch (error) {
      console.error('Error fetching balance:', error);
      await bot.sendMessage(chatId, getMessage('balanceError'));
  }
}


async function handleAnalizCommand(chatId, text) {
  try {
    const subsinfo = await checkIsUserSubscriber(chatId);
    // Parse args: /analiz BTC 1H
    const parts = text.trim().split(/\s+/);
    const symbol = parts[1] || 'BTC';
    const tf = parts[2] || '1h';

    // Eğer sadece "/analiz" (veya "/analiz@...") yazılmışsa
    if (parts.length === 1 || (parts.length === 2 && parts[1].startsWith('@'))) {
      const fs = require('fs');
      const path = require('path');
      try {
        const helpPath = path.join(__dirname, '..', 'templates', 'help_analiz.md');
        const fallbackPath = path.join(__dirname, 'templates', 'help_analiz.md');
        const p = fs.existsSync(helpPath) ? helpPath : fallbackPath;
        const helpText = fs.readFileSync(p, 'utf8');
        await bot.sendMessage(chatId, helpText, { parse_mode: 'Markdown' });
        return;
      } catch (e) {
        // Fallback mesaj
        await bot.sendMessage(chatId, `
⚡ /analiz Kullanımı

Ürün kısaltması + zaman dilimi ile analiz isteyebilirsiniz.

🔹 Örnek:
/analiz BTC / 1H → Bitcoin için 1 saatlik grafik analizi
/analiz ETH / 4H → Ethereum için 4 saatlik grafik analizi
/analiz XRP / 1D → XRP için günlük grafik analizi

📊 Alexa, seçtiğiniz ürün ve zaman diliminde teknik analiz + yapay zekâ yorumunu sunacaktır.
        `, { parse_mode: 'Markdown' });
        return;
      }
    }

    // Eğer kullanıcı abonelik sahibi değilse free limit kontrolü
    if (!subsinfo.isSubscriber) {
      await ensureFreeUser(chatId);
      const allowed = await canUseAnaliz(chatId);
      if (!allowed) {
        return bot.sendMessage(
          chatId,
          getMessage('freeAnalizLimitReached') ||
            '🆓 Free üyelikte /analiz günde 1 kez kullanılabilir. Sınırsız kullanım için lütfen abone olun.'
        );
      }
    }

    await bot.sendMessage(chatId, getMessage('analizStarting') || '⏳ Analiz hazırlanıyor, lütfen bekleyin...');

    const { text: report } = await performAnalysis(symbol, tf);

    await bot.sendMessage(chatId, report, { parse_mode: 'HTML', disable_web_page_preview: true });

    if (!subsinfo.isSubscriber) {
      await recordAnalizUse(chatId);
    }
  } catch (e) {
    console.error('[Analiz] error:', e);
    await bot.sendMessage(chatId, getMessage('errorOccurred') || '⚠️ Bir hata oluştu, lütfen daha sonra tekrar deneyin.');
  }
}


module.exports = {
  sendMessageToSubscribers,
  startPolling,
  stopPolling,
  bot
};