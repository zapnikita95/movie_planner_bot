// content/content-letterboxd.js - Парсинг imdb_id с Letterboxd

function extractImdbId() {
  // Ищем все <a> с href на imdb.com/title/tt...
  const links = document.querySelectorAll('a[href*="imdb.com/title/tt"]');
  
  for (const link of links) {
    const match = link.href.match(/imdb\.com\/title\/(tt\d+)/i);
    if (match) {
      return match[1];  // tt32916440
    }
  }
  
  // Альтернатива: ищем в футере или в "More at IMDb"
  const moreLink = document.querySelector('a[href*="imdb.com/title"]');
  if (moreLink) {
    const match = moreLink.href.match(/tt\d+/);
    if (match) return match[0];
  }
  
  // Пробуем селектор из примера пользователя
  const selector = '#film-page-wrapper > div.col-17 > section.section.col-10.col-main > p > a:nth-child(1)';
  const imdbLink = document.querySelector(selector);
  if (imdbLink && imdbLink.href) {
    const match = imdbLink.href.match(/imdb\.com\/title\/(tt\d+)/i);
    if (match) {
      return match[1];
    }
  }
  
  return null;
}

function extractTitleAndYear() {
  // Парсим название
  // Селектор: #film-page-wrapper > div.col-17 > section.production-masthead.-shadowed.-productionscreen.-film > div > h1 > span
  const titleElement = document.querySelector('#film-page-wrapper h1 span.name');
  let title = null;
  if (titleElement) {
    title = titleElement.textContent.trim();
    // Очищаем от HTML entities и нормализуем
    title = title.replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&apos;/g, "'").replace(/&quot;/g, '"').replace(/\s+/g, ' ').trim();
  }
  
  // Парсим год
  // Селектор: #film-page-wrapper > div.col-17 > section.production-masthead.-shadowed.-productionscreen.-film > div > div > span > a
  const yearElement = document.querySelector('#film-page-wrapper section.production-masthead div span a[href*="/films/year/"]');
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
  if (message.action === 'get_letterboxd_title_year') {
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
