const { dbQuery } = require('./db');

// Ensure a user exists in free_users table
async function ensureFreeUser(user_id) {
  try {
    await dbQuery(`
      INSERT IGNORE INTO free_users (user_id, created_at, analiz_count, last_analiz_at)
      VALUES (?, NOW(), 0, NULL)
    `, [user_id]);
    return true;
  } catch (e) {
    console.error('[FreeTier] ensureFreeUser error:', e);
    return false;
  }
}

// List all free users
async function getFreeUsers() {
  try {
    const rows = await dbQuery('SELECT user_id FROM free_users');
    return rows.map(r => r.user_id);
  } catch (e) {
    console.error('[FreeTier] getFreeUsers error:', e);
    return [];
  }
}

// Increment global signal counter and return whether this signal should be sent to free users
async function incrementAndShouldSend() {
  try {
    await dbQuery(`
      INSERT INTO system_counters (name, value)
      VALUES ('signal_counter', 1)
      ON DUPLICATE KEY UPDATE value = value + 1
    `);
    const [row] = await dbQuery("SELECT value FROM system_counters WHERE name = 'signal_counter'");
    const counter = row ? parseInt(row.value, 10) : 1;
    // Every 20th signal
    return counter % 20 === 0;
  } catch (e) {
    console.error('[FreeTier] incrementAndShouldSend error:', e);
    // Fallback: do not send to free if counter unavailable
    return false;
  }
}

// Check if a free user can use /analiz today (1 per day)
async function canUseAnaliz(user_id) {
  try {
    const [row] = await dbQuery('SELECT analiz_count, last_analiz_at FROM free_users WHERE user_id = ?', [user_id]);
    if (!row) {
      // Not in table -> allow once (caller should ensureFreeUser beforehand)
      return true;
    }
    const today = new Date().toISOString().slice(0,10);
    const last = row.last_analiz_at ? new Date(row.last_analiz_at) : null;
    const lastDay = last ? last.toISOString().slice(0,10) : null;
    if (lastDay !== today) return true;
    return (parseInt(row.analiz_count || 0, 10) < 1);
  } catch (e) {
    console.error('[FreeTier] canUseAnaliz error:', e);
    return false;
  }
}

// Record an analiz usage for free user
async function recordAnalizUse(user_id) {
  try {
    const today = new Date().toISOString().slice(0,10);
    // If last_analiz_at is today -> increment, else reset to 1
    const rows = await dbQuery('SELECT analiz_count, last_analiz_at FROM free_users WHERE user_id = ?', [user_id]);
    if (rows.length === 0) {
      await dbQuery('INSERT INTO free_users (user_id, created_at, analiz_count, last_analiz_at) VALUES (?, NOW(), 1, NOW())', [user_id]);
      return;
    }
    const row = rows[0];
    const last = row.last_analiz_at ? new Date(row.last_analiz_at) : null;
    const lastDay = last ? last.toISOString().slice(0,10) : null;
    if (lastDay === today) {
      await dbQuery('UPDATE free_users SET analiz_count = analiz_count + 1 WHERE user_id = ?', [user_id]);
    } else {
      await dbQuery('UPDATE free_users SET analiz_count = 1, last_analiz_at = NOW() WHERE user_id = ?', [user_id]);
    }
  } catch (e) {
    console.error('[FreeTier] recordAnalizUse error:', e);
  }
}

module.exports = {
  ensureFreeUser,
  getFreeUsers,
  incrementAndShouldSend,
  canUseAnaliz,
  recordAnalizUse
};