// content/content-kinorium.js — парсинг страницы фильма/сериала на ru.kinorium.com
// При клике на расширение: title, year, isSeries; поиск по "title year" как на letterboxd.

const PREFIX = '[MoviePlanner kinorium]';

function getKinoriumFilmInfo() {
  let title = null;
  let year = null;
  let isSeries = null;

  // Название
  const titleEl = document.querySelector(
    'body > div.modalBluring > div > div.table.main-table_content.main-table.main-container_experimentFixedMenu > div.table-row > div.table-cell.centerContent.centerContent_with-right-sidebar > section > div.container.film-page.headlinesShow > div.film-page__title.film-page__title_with-copylink > div.film-page__title-elements > div > h1'
  );
  if (titleEl) {
    title = (titleEl.textContent || '').trim();
    if (title) console.log(PREFIX, 'Название:', title);
  } else {
    const h1 = document.querySelector('.film-page__title-text, .film-page h1[itemprop="name"]');
    if (h1) title = (h1.textContent || '').trim();
    if (!title) console.log(PREFIX, 'Элемент с названием не найден');
  }

  // Год — из span > span > span > a
  const yearEl = document.querySelector(
    'body > div.modalBluring > div > div.table.main-table_content.main-table.main-container_experimentFixedMenu > div.table-row > div.table-cell.centerContent.centerContent_with-right-sidebar > section > div.container.film-page.headlinesShow > div.film-page__title.film-page__title_with-copylink > div.film-page__title-elements > div > span > span > span > a'
  );
  if (yearEl) {
    const y = (yearEl.textContent || '').trim().match(/\d{4}/);
    if (y) {
      year = y[0];
      console.log(PREFIX, 'Год:', year);
    }
  } else {
    const yearLink = document.querySelector('.film-page__title-elements span span span a[href*="years_min"]');
    if (yearLink) {
      const ym = (yearLink.textContent || '').match(/\d{4}/);
      if (ym) year = ym[0];
    }
    if (!year) console.log(PREFIX, 'Элемент с годом не найден');
  }

  // Сериал или фильм — вкладка (Сериал / Фильм)
  const tabEl = document.querySelector(
    'body > div.modalBluring > div > div.table.main-table_content.main-table.main-container_experimentFixedMenu > div.table-row > div.table-cell.centerContent.centerContent_with-right-sidebar > section > div.container.film-page.headlinesShow > ul.tabs-subpage > li:nth-child(1) > a'
  );
  if (tabEl) {
    const text = (tabEl.textContent || '').trim().toLowerCase();
    isSeries = text.includes('сериал');
    console.log(PREFIX, 'Тип из вкладки:', isSeries ? 'сериал' : 'фильм');
  } else {
    const tab = document.querySelector('.film-page .tabs-subpage li:nth-child(1) a');
    if (tab) isSeries = (tab.textContent || '').toLowerCase().includes('сериал');
    if (isSeries === null) console.log(PREFIX, 'Элемент вкладки не найден');
  }

  return { title, year, isSeries };
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'get_kinorium_film_info') {
    const info = getKinoriumFilmInfo();
    sendResponse(info);
    return true;
  }
});
