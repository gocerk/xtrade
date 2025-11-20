// generate_hash.js
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// server.js dosyasının tam yolunu al
const serverPath = path.join(__dirname, 'server.js');

// Hash hesapla
const fileBuffer = fs.readFileSync(serverPath);
const hashSum = crypto.createHash('sha256');
hashSum.update(fileBuffer);
const hash = hashSum.digest('hex');

// Konsola yazdır
console.log('\nServer.js Hash Değeri:');
console.log(hash);

// JSON formatında
console.log('\nJSON Formatı:');
console.log(JSON.stringify({ hash: hash }, null, 2));

// Opsiyonel: Hash'i bir dosyaya kaydet
const jsonContent = JSON.stringify({ hash: hash }, null, 2);
fs.writeFileSync('server_hash.json', jsonContent);
console.log('\nHash değeri server_hash.json dosyasına kaydedildi.');