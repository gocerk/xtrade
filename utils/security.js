// utils/security.js
const crypto = require('crypto');
const axios = require('axios');
const os = require('os');

// Endpoint şifreleme
const LICENSE_ENDPOINT = Buffer.from('aHR0cHM6Ly93ZWJ6YS5uZXQvbGljZW5zZXMvYWxsb3dlZF9pcHMuanNvbg==', 'base64').toString('ascii');
const HASH_ENDPOINT = Buffer.from('aHR0cHM6Ly93ZWJ6YS5uZXQvbGljZW5zZXMvaGFzaGVzLmpzb24=', 'base64').toString('ascii');

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
        console.log('[Security] Current IP:', currentIP);

        const response = await axios.get(LICENSE_ENDPOINT);
        const allowedIPs = response.data.ips;

        if (!allowedIPs.includes(currentIP)) {
            console.error('[Security] Unauthorized IP address:', currentIP);
            return false;
        }
        
        return true;
    } catch (error) {
        console.error('[Security] License check failed:', error.message);
        return false;
    }
}

async function validateFileHashes() {
    try {
        const files = ['bot.js', 'server.js', 'webhook.js'];
        const response = await axios.get(HASH_ENDPOINT);
        const validHashes = response.data;
        
        for (const file of files) {
            const fileBuffer = fs.readFileSync(file);
            const hashSum = crypto.createHash('sha256');
            hashSum.update(fileBuffer);
            const hash = hashSum.digest('hex');
            
            if (hash !== validHashes[file]) {
                console.error(`[Security] File integrity check failed for ${file}`);
                return false;
            }
        }
        return true;
    } catch (error) {
        console.error('[Security] Hash validation failed:', error.message);
        return false;
    }
}

async function validateSecurity() {
    const ipValid = await checkIPAuthorization();
    const hashValid = await validateFileHashes();
    return ipValid && hashValid;
}

module.exports = {
    validateSecurity,
    checkIPAuthorization,
    validateFileHashes
};