// content/content-kp.js - Парсинг Кинопоиска

function extractKpId() {
  // Извлекаем kp_id из URL
  const urlMatch = window.location.href.match(/kinopoisk\.ru\/(film|series)\/(\d+)/i);
  if (urlMatch) {
    return {
      kp_id: urlMatch[2],
      is_series: urlMatch[1] === 'series'
    };
  }
  
  return null;
}

// Отправляем kp_id в background script
const kpData = extractKpId();

// Обработчик запроса от popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'get_kp_id') {
    sendResponse({ kpId: kpData ? kpData.kp_id : null, isSeries: kpData ? kpData.is_series : false });
    return true;
  }
});

if (kpData) {
  chrome.runtime.sendMessage({ 
    action: "found_kp_id", 
    kp_id: kpData.kp_id,
    is_series: kpData.is_series
  });
}
