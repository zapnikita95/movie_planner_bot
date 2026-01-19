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

// Отправляем imdb_id в background script
const imdbId = extractImdbId();
if (imdbId) {
  chrome.runtime.sendMessage({ 
    action: "found_imdb_id", 
    imdbId: imdbId 
  });
} else {
  // Fallback: отправляем название и год
  const titleElem = document.querySelector('h1.filmtitle, h1.headline-1');
  const yearElem = document.querySelector('small.releaseyear, .releaseyear');
  
  if (titleElem && yearElem) {
    const title = titleElem.textContent.trim();
    const year = yearElem.textContent.replace(/[()]/g, '').trim();
    
    chrome.runtime.sendMessage({
      action: "letterboxd_fallback",
      title: title,
      year: year
    });
  }
}
