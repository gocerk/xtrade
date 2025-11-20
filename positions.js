const Binance = require('binance-api-node').default;
const { getUserApiKeys } = require('./utils');
const path = require('path');
const fs = require('fs').promises;  // promises versiyonunu kullanıyoruz
const { positionTrackers } = require('./binance_operations'); // Yeni eklenen


async function showPositions(chatId, bot) {
  try {
    const userApiKeys = await getUserApiKeys(chatId);
    if (!userApiKeys) {
      return bot.sendMessage(chatId, 'API anahtarlarınız bulunamadı. Lütfen önce API anahtarlarınızı ayarlayın.');
    }

    const client = Binance({
      apiKey: userApiKeys.apiKey,
      apiSecret: userApiKeys.apiSecret,
      futures: true
    });

    const positions = await client.futuresPositionRisk();
    const openPositions = positions.filter(position => parseFloat(position.positionAmt) !== 0);

    if (openPositions.length === 0) {
      await bot.sendMessage(chatId, 'Şu anda açık futures pozisyonunuz bulunmamaktadır.');
    } else {
      const groupedPositions = openPositions.reduce((acc, position) => {
        if (!acc[position.symbol]) {
          acc[position.symbol] = {
            LONG: null,
            SHORT: null
          };
        }
        acc[position.symbol][position.positionSide] = position;
        return acc;
      }, {});

      for (const [symbol, positions] of Object.entries(groupedPositions)) {
        let message = `📊 ${symbol} Pozisyon Detayları:\n`;
        let totalUnrealizedPnl = 0;
        let buttons = [];

        for (const side of ['LONG', 'SHORT']) {
          const position = positions[side];
          if (position) {
            const amount = Math.abs(parseFloat(position.positionAmt));
            const entryPrice = parseFloat(position.entryPrice);
            const markPrice = parseFloat(position.markPrice);
            const unrealizedPnl = parseFloat(position.unRealizedProfit);
            const leverage = parseFloat(position.leverage);
            const roe = (unrealizedPnl / (amount * entryPrice / leverage)) * 100;

            totalUnrealizedPnl += unrealizedPnl;

            message += `\n${side === 'LONG' ? '📈' : '📉'} ${side} Pozisyon:
💰 Miktar: ${amount}
⚡ Kaldıraç: ${leverage}x
🚀 Giriş Fiyatı: ${entryPrice.toFixed(8)}
📈 Mevcut Fiyat: ${markPrice.toFixed(8)}
💵 Gerçekleşmemiş Kâr/Zarar: ${unrealizedPnl.toFixed(2)} USDT
📊 ROE: ${roe.toFixed(2)}%\n`;

            buttons.push([{ 
              text: `${side} Pozisyonu Kapat`, 
              callback_data: `close_position_${symbol}_${side}` 
            }]);
          }
        }

        message += `\n💹 Toplam Gerçekleşmemiş Kâr/Zarar: ${totalUnrealizedPnl.toFixed(2)} USDT`;

        await bot.sendMessage(chatId, message, {
          reply_markup: {
            inline_keyboard: buttons
          }
        });
      }
    }
  } catch (error) {
    console.error('Error fetching futures positions:', error);
    bot.sendMessage(chatId, 'Futures pozisyonları alınırken bir hata oluştu. - Lütfen tekrar deneyin. \n Spam yapmayın, üst üste binance butonlarına basmayın. Yoksa bu hatayı alabilirsiniz.');
  }
}

async function closePosition(chatId, symbol, side, bot) {
  try {
    const userApiKeys = await getUserApiKeys(chatId);
    if (!userApiKeys) {
      return bot.sendMessage(chatId, 'API anahtarlarınız bulunamadı. Lütfen önce API anahtarlarınızı ayarlayın.');
    }

    const client = Binance({
      apiKey: userApiKeys.apiKey,
      apiSecret: userApiKeys.apiSecret,
      futures: true
    });

    const positions = await client.futuresPositionRisk({ symbol });
    const position = positions.find(p => p.positionSide === side && parseFloat(p.positionAmt) !== 0);

    if (!position) {
      return bot.sendMessage(chatId, `${symbol} için ${side} pozisyonu bulunamadı.`);
    }

    console.log(`Closing ${side} position for ${symbol}`);

    try {
      // Önce emirleri iptal et
      await cancelRelatedOrders(client, symbol, side, chatId);
      console.log(`Successfully cancelled related orders for ${symbol} ${side}`);
    } catch (error) {
      console.error(`Error cancelling orders: ${error.message}`);
      // Emirleri iptal edemesek bile pozisyonu kapatmaya devam edelim
    }

    // Pozisyonu kapat
    const quantity = Math.abs(parseFloat(position.positionAmt));
    const order = await client.futuresOrder({
      symbol: symbol,
      side: side === 'LONG' ? 'SELL' : 'BUY',
      type: 'MARKET',
      quantity: quantity.toFixed(8),
      positionSide: side
    });

    // positionTracker'ı temizle
    const trackerKey = `${chatId}_${symbol}_${side}`;
    if (positionTrackers.has(trackerKey)) {
      const tracker = positionTrackers.get(trackerKey);
      await tracker.destroy();
      positionTrackers.delete(trackerKey);
      console.log(`Tracker cleaned up for ${symbol} ${side}`);
    }

    const pnl = parseFloat(position.unRealizedProfit);
    const entryPrice = parseFloat(position.entryPrice);
    const pnlPercent = (pnl / (quantity * entryPrice / parseFloat(position.leverage))) * 100;

    const pnlMessage = pnl >= 0 ? 
      `📈 Kâr: ${pnl.toFixed(2)} USDT` :
      `📉 Zarar: ${Math.abs(pnl).toFixed(2)} USDT`;

    const message = `✅ ${symbol} ${side} pozisyonu başarıyla kapatıldı.
💰 Kapatılan Miktar: ${quantity} ${symbol}
${pnlMessage}`;

    bot.sendMessage(chatId, message);
  } catch (error) {
    console.error('Error closing futures position:', error);
    bot.sendMessage(chatId, `❌ ${symbol} ${side} pozisyonu kapatılırken bir hata oluştu. \n Lütfen tekrar deneyin. Spam yapmayın, üst üste binance butonlarına basmayın. Yoksa bu hatayı alabilirsiniz.`);
  }
}

async function cancelAllOrders(client, symbol) {
  try {
    await client.futuresCancelAllOpenOrders({ symbol });
    console.log(`Cancelled all open orders for ${symbol}`);
  } catch (error) {
    console.error(`Error cancelling all open orders for ${symbol}:`, error);
  }
}

async function cancelRelatedOrders(client, symbol, side, chatId) {
  try {
    console.log(`Starting to cancel orders for ${symbol} ${side} position`);
    
    // Önce tüm açık emirleri al
    const openOrders = await client.futuresOpenOrders({ symbol });
    if (openOrders.length === 0) {
      console.log(`No open orders found for ${symbol}`);
      return;
    }

    console.log(`Found ${openOrders.length} open orders for ${symbol}`);

    // Pozisyon tarafına göre emirleri filtrele
    const ordersToCancel = openOrders.filter(order => 
      (order.type === 'TAKE_PROFIT_MARKET' || order.type === 'STOP_MARKET') && 
      order.positionSide === side
    );

    if (ordersToCancel.length === 0) {
      console.log(`No orders to cancel for ${symbol} ${side}`);
      return;
    }

    console.log(`Cancelling ${ordersToCancel.length} orders for ${symbol} ${side}`);

    // Emirleri iptal et
    for (const order of ordersToCancel) {
      try {
        await client.futuresCancelOrder({
          symbol: symbol,
          orderId: order.orderId
        });
        console.log(`Successfully cancelled order ${order.orderId} for ${symbol} ${side}`);
      } catch (error) {
        console.error(`Error cancelling order ${order.orderId}:`, error.message);
      }
    }

    // userOrders dosyasını güncelle
    try {
      const userOrdersFile = path.join(__dirname, 'userOrders', `${chatId}.json`);
      let userOrders = {};
      
      try {
        const data = await fs.readFile(userOrdersFile, 'utf8');
        userOrders = JSON.parse(data);
      } catch (error) {
        if (error.code !== 'ENOENT') {
          console.error(`Error reading user orders for ${chatId}:`, error);
        }
      }

      if (userOrders[symbol]) {
        if (side === 'LONG') {
          delete userOrders[symbol].tp1_long;
          delete userOrders[symbol].tp2_long;
          delete userOrders[symbol].sl_long;
        } else {
          delete userOrders[symbol].tp1_short;
          delete userOrders[symbol].tp2_short;
          delete userOrders[symbol].sl_short;
        }

        if (Object.keys(userOrders[symbol]).length === 0) {
          delete userOrders[symbol];
        }

        await fs.writeFile(userOrdersFile, JSON.stringify(userOrders, null, 2));
        console.log(`Updated userOrders file for ${symbol} ${side}`);
      }
    } catch (error) {
      console.error(`Error updating userOrders file:`, error);
    }

  } catch (error) {
    console.error(`Error in cancelRelatedOrders for ${symbol} ${side}:`, error);
  }
}

module.exports = { showPositions, closePosition };