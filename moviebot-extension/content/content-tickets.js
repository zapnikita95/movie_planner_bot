// content/content-tickets.js - Обработка сайтов с билетами

// Определяем, что мы на сайте с билетами
const ticketSites = [
  'afisha.yandex.ru',
  'www.afisha.ru',
  'www.kinopoisk.ru',
  'kinoteatr.ru',
  'kinoafisha.info',
  'karofilm.ru'
];

const currentHost = window.location.hostname;
const isTicketSite = ticketSites.some(site => currentHost.includes(site));

if (isTicketSite) {
  // Уведомляем background script
  chrome.runtime.sendMessage({
    action: "found_ticket_site",
    url: window.location.href
  });
  
  // Добавляем кнопку на страницу
  addMoviePlannerButton();
}

function addMoviePlannerButton() {
  // Проверяем, не добавлена ли уже кнопка
  if (document.getElementById('movie-planner-btn')) {
    return;
  }
  
  // Создаем кнопку
  const button = document.createElement('button');
  button.id = 'movie-planner-btn';
  button.textContent = '🎫 Отправить в Movie Planner';
  button.style.cssText = `
    position: fixed;
    bottom: 20px;
    right: 20px;
    z-index: 10000;
    padding: 12px 20px;
    background: #007bff;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    transition: background 0.2s;
  `;
  
  button.addEventListener('mouseenter', () => {
    button.style.background = '#0056b3';
  });
  
  button.addEventListener('mouseleave', () => {
    button.style.background = '#007bff';
  });
  
  button.addEventListener('click', async () => {
    // Проверяем авторизацию
    const data = await chrome.storage.local.get(['linked_chat_id']);
    if (!data.linked_chat_id) {
      alert('Сначала авторизуйтесь в расширении!');
      chrome.runtime.sendMessage({ action: 'open_popup' });
      return;
    }
    
    // Открываем popup для загрузки билета
    chrome.runtime.sendMessage({
      action: 'open_ticket_upload',
      url: window.location.href
    });
  });
  
  document.body.appendChild(button);
}
