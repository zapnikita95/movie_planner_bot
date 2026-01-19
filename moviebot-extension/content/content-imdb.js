// content/content-imdb.js - Парсинг imdb_id с IMDb

function extractImdbId() {
  // Извлекаем imdb_id из URL
  const urlMatch = window.location.href.match(/imdb\.com\/title\/(tt\d+)/i);
  if (urlMatch) {
    return urlMatch[1];
  }
  
  return null;
}

// Отправляем imdb_id в background script
const imdbId = extractImdbId();

// Обработчик запроса от popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'get_imdb_id') {
    sendResponse({ imdbId: imdbId || null });
    return true;
  }
});

if (imdbId) {
  chrome.runtime.sendMessage({ 
    action: "found_imdb_id", 
    imdbId: imdbId 
  });
}
