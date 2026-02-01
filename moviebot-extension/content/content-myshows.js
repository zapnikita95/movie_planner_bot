// content/content-myshows.js — парсинг страницы сериала/фильма на myshows.me
// При клике на расширение: kp_id из ссылки на Кинопоиск или fallback title + year; тип (фильм/сериал) из API.

const PREFIX = '[MoviePlanner myshows]';

function getMyshowsFilmInfo() {
  let kpId = null;
  let title = null;
  let year = null;
  let isSeries = null;

  // 1. ПРЕЖДЕ ВСЕГО — id Кинопоиска из ссылки (строка таблицы с Кинопоиском)
  const kpLink = document.querySelector(
    '#__nuxt > div.LayoutWrapper.isDesktop.show > div.LayoutWrapper__main > div > div > div.DefaultLayout__content > div > main > div.ShowDetails > div:nth-child(4) > div:nth-child(2) > table > tbody > tr:nth-child(10) > td.info-row__value > a'
  );
  if (kpLink && kpLink.href) {
    const match = kpLink.href.match(/kinopoisk\.ru\/(?:film|series)\/(\d+)/i);
    if (match) {
      kpId = match[1];
      console.log(PREFIX, 'kp_id из ссылки:', kpId);
    }
  }
  if (!kpId) {
    // Запасной поиск ссылки на Кинопоиск в таблице
    const anyKp = document.querySelector('.ShowDetails a[href*="kinopoisk.ru/film"], .ShowDetails a[href*="kinopoisk.ru/series"]');
    if (anyKp && anyKp.href) {
      const m = anyKp.href.match(/kinopoisk\.ru\/(?:film|series)\/(\d+)/i);
      if (m) {
        kpId = m[1];
        console.log(PREFIX, 'kp_id из запасной ссылки:', kpId);
      }
    }
  }
  if (!kpId) {
    console.log(PREFIX, 'Элемент со ссылкой на Кинопоиск не найден или поиск не сработал');
  }

  // 2. Fallback: название
  const titleEl = document.querySelector(
    '#__nuxt > div.LayoutWrapper.isDesktop.show > div.LayoutWrapper__main > div > div > div.DefaultLayout__content > div > main > div.ShowDetails > div:nth-child(1) > div.title.title__primary.title--left.title__space-m.ShowDetails-title > div.title__main > h1'
  );
  if (titleEl) {
    title = (titleEl.textContent || '').trim();
    if (title) console.log(PREFIX, 'Название (fallback):', title);
  } else {
    const h1 = document.querySelector('.ShowDetails .title__main-text, .ShowDetails-title h1');
    if (h1) title = (h1.textContent || '').trim();
    if (!title) console.log(PREFIX, 'Элемент с названием не найден');
  }

  // 3. Fallback: год — первая строка таблицы, только ПЕРВОЕ упоминание года (4 цифры)
  const yearCell = document.querySelector(
    '#__nuxt > div.LayoutWrapper.isDesktop.show > div.LayoutWrapper__main > div > div > div.DefaultLayout__content > div > main > div.ShowDetails > div:nth-child(4) > div:nth-child(2) > table > tbody > tr:nth-child(1) > td.info-row__value'
  );
  if (yearCell) {
    const text = (yearCell.textContent || '').trim();
    const yearMatch = text.match(/\d{4}/);
    if (yearMatch) {
      year = yearMatch[0];
      console.log(PREFIX, 'Год (fallback):', year);
    }
  } else {
    const firstRow = document.querySelector('.ShowDetails table tbody tr:nth-child(1) td.info-row__value');
    if (firstRow) {
      const ym = (firstRow.textContent || '').match(/\d{4}/);
      if (ym) year = ym[0];
    }
    if (!year) console.log(PREFIX, 'Элемент с годом не найден');
  }

  // 4. Сериал или фильм — из хлебных крошек
  const breadcrumbLink = document.querySelector(
    '#__nuxt > div.LayoutWrapper.isDesktop.show > div.LayoutWrapper__main > div > div > div.DefaultLayout__content > div > main > div.ShowDetails > div:nth-child(1) > ul > li:nth-child(2) > a'
  );
  if (breadcrumbLink) {
    const text = (breadcrumbLink.textContent || '').trim().toLowerCase();
    if (text === 'сериалы' || text === 'series') {
      isSeries = true;
    } else {
      isSeries = false;
    }
    console.log(PREFIX, 'Тип из хлебных крошек:', isSeries ? 'сериал' : 'фильм');
  } else {
    const crumb = document.querySelector('.ShowDetails ul li:nth-child(2) a');
    if (crumb) {
      const t = (crumb.textContent || '').trim().toLowerCase();
      isSeries = t === 'сериалы' || t === 'series';
    }
    if (isSeries === null) console.log(PREFIX, 'Элемент хлебных крошек не найден');
  }

  return { kpId, title, year, isSeries };
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'get_myshows_film_info') {
    const info = getMyshowsFilmInfo();
    sendResponse(info);
    return true;
  }
});
