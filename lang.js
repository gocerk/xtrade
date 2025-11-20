// lang.js

const fs = require('fs').promises;
const path = require('path');

const languageFile = path.join(__dirname, 'language.json');

let lang = {};

async function loadLanguageFile() {
  try {
    const data = await fs.readFile(languageFile, 'utf8');
    lang = JSON.parse(data);
    console.log('Dil dosyası başarıyla yüklendi.');
  } catch (e) {
    console.error('Dil dosyası yüklenirken hata oluştu:', e);
    // Hata durumunda varsayılan bir dil objesi oluşturabilirsiniz
    lang = { error: 'Dil dosyası yüklenemedi' };
  }
}

// Çok seviyeli anahtarları işlemek için yardımcı fonksiyon
function getNested(obj, key) {
  return key.split('.').reduce((o, k) => (o && k in o) ? o[k] : undefined, obj);
}

function getMessage(key, variables = {}) {
  let message = getNested(lang, key);
  if (typeof message === 'undefined') {
    console.warn(`'${key}' için tanımlı mesaj bulunamadı.`);
    message = key;
  }
  for (const [k, v] of Object.entries(variables)) {
    const placeholder = '{{' + k + '}}';
    message = message.split(placeholder).join(v);
  }
  return message;
}

// Dil dosyasını yeniden yükleme fonksiyonu
async function reloadLanguageFile() {
  await loadLanguageFile();
}

// İlk yüklemeyi gerçekleştir
loadLanguageFile();

module.exports = {
  getMessage,
  lang,
  reloadLanguageFile
};