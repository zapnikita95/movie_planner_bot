// content/content-imdb.js - Парсинг imdb_id с IMDb

function extractImdbId() {
  // Извлекаем imdb_id из URL
  const urlMatch = window.location.href.match(/imdb\.com\/title\/(tt\d+)/i);
  if (urlMatch) {
    return urlMatch[1];
  }
  
  return null;
}

function extractTitleAndYear() {
  // Парсим original title
  // Селектор: #__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-358297d7-0.CHcbB > section > div:nth-child(5) > section > section > div.sc-14a487d5-3.ckSjzt > div.sc-af040695-0.iOwuHP > div
  const titleElement = document.querySelector('div.sc-b41e510f-2.jUfqFl.baseAlt');
  let title = null;
  if (titleElement && titleElement.textContent.includes('Original title:')) {
    title = titleElement.textContent.replace('Original title:', '').trim();
    // Очищаем от HTML entities и нормализуем
    title = title.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
  }
  
  // Парсим год
  // Селектор: #__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-358297d7-0.CHcbB > section > div:nth-child(5) > section > section > div.sc-14a487d5-3.ckSjzt > div.sc-af040695-0.iOwuHP > ul > li:nth-child(1) > a
  const yearElement = document.querySelector('ul.ipc-inline-list li a[href*="/releaseinfo"]');
  let year = null;
  if (yearElement) {
    const yearText = yearElement.textContent.trim();
    const yearMatch = yearText.match(/\d{4}/);
    if (yearMatch) {
      year = parseInt(yearMatch[0]);
    }
  }
  
  return { title, year };
}

// Отправляем imdb_id в background script
const imdbId = extractImdbId();

// Обработчик запроса от popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'get_imdb_id') {
    sendResponse({ imdbId: imdbId || null });
    return true;
  }
  if (message.action === 'get_imdb_title_year') {
    const { title, year } = extractTitleAndYear();
    sendResponse({ title, year });
    return true;
  }
});

if (imdbId) {
  chrome.runtime.sendMessage({ 
    action: "found_imdb_id", 
    imdbId: imdbId 
  });
}
