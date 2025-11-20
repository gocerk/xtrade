// signalTracker.js
const Binance = require('node-binance-api');
const binance = new Binance().options({});

async function trackSignals() {
    try {
        // Aktif sinyalleri al
        const activeSignals = await dbQuery(
            'SELECT * FROM signal_results WHERE status = ?',
            ['ACTIVE']
        );

        for (const signal of activeSignals) {
            // Binance'den güncel fiyatı al
            const ticker = await binance.prices();
            const currentPrice = parseFloat(ticker[signal.coin]);

            if (!currentPrice) continue;

            let status = 'ACTIVE';
            let hitTP = false;
            let hitSL = false;

            // Stop Loss kontrolü
            if (signal.action.toLowerCase() === 'buy' || signal.action.toLowerCase() === 'long') {
                if (currentPrice <= signal.stop_loss) {
                    hitSL = true;
                    status = 'STOPPED';
                } else if (currentPrice >= signal.tp1_price) {
                    signal.hit_tp1 = true;
                    if (currentPrice >= signal.tp2_price) {
                        signal.hit_tp2 = true;
                        if (currentPrice >= signal.tp3_price) {
                            signal.hit_tp3 = true;
                            status = 'COMPLETED';
                        }
                    }
                    hitTP = true;
                }
            } else { // Sell/Short için
                if (currentPrice >= signal.stop_loss) {
                    hitSL = true;
                    status = 'STOPPED';
                } else if (currentPrice <= signal.tp1_price) {
                    signal.hit_tp1 = true;
                    if (currentPrice <= signal.tp2_price) {
                        signal.hit_tp2 = true;
                        if (currentPrice <= signal.tp3_price) {
                            signal.hit_tp3 = true;
                            status = 'COMPLETED';
                        }
                    }
                    hitTP = true;
                }
            }

            // Kar/zarar hesaplama
            const profitPercentage = signal.action.toLowerCase() === 'buy' || signal.action.toLowerCase() === 'long'
                ? ((currentPrice - signal.entry_price) / signal.entry_price) * 100
                : ((signal.entry_price - currentPrice) / signal.entry_price) * 100;

            // Veritabanını güncelle
            await dbQuery(`
                UPDATE signal_results 
                SET 
                    current_price = ?,
                    hit_sl = ?,
                    hit_tp1 = ?,
                    hit_tp2 = ?,
                    hit_tp3 = ?,
                    profit_percentage = ?,
                    status = ?,
                    completed_at = ?
                WHERE id = ?`,
                [
                    currentPrice,
                    hitSL,
                    signal.hit_tp1,
                    signal.hit_tp2,
                    signal.hit_tp3,
                    profitPercentage,
                    status,
                    status !== 'ACTIVE' ? new Date() : null,
                    signal.id
                ]
            );
        }
    } catch (error) {
        console.error('Signal tracking error:', error);
    }
}

// Her 5 saniyede bir kontrol et
setInterval(trackSignals, 5000);