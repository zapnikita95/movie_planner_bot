// content-tickets.js
// Content script для билетных сайтов: добавляет кнопку "Добавить билеты к плану" рядом с кнопками скачивания билетов

(function() {
  'use strict';
  
  // Функция создания кнопки добавления билетов
  function createAddTicketsButton(downloadButton) {
    // Проверяем, не создана ли уже кнопка
    if (downloadButton.nextElementSibling && 
        downloadButton.nextElementSibling.classList && 
        downloadButton.nextElementSibling.classList.contains('movieplanner-add-tickets-btn')) {
      return; // Кнопка уже существует
    }
    
    const addTicketsBtn = document.createElement('button');
    addTicketsBtn.textContent = '🎟️ Добавить билеты к плану';
    addTicketsBtn.className = 'movieplanner-add-tickets-btn';
    addTicketsBtn.style.cssText = `
      margin-left: 10px;
      padding: 8px 16px;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      transition: all 0.3s;
      box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);
    `;
    
    addTicketsBtn.addEventListener('mouseenter', () => {
      addTicketsBtn.style.transform = 'translateY(-2px)';
      addTicketsBtn.style.boxShadow = '0 4px 12px rgba(102, 126, 234, 0.4)';
    });
    
    addTicketsBtn.addEventListener('mouseleave', () => {
      addTicketsBtn.style.transform = 'translateY(0)';
      addTicketsBtn.style.boxShadow = '0 2px 8px rgba(102, 126, 234, 0.3)';
    });
    
    addTicketsBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      
      // Получаем информацию о билете (изображение, ссылка и т.д.)
      try {
        // Ищем изображение билета или ссылку на скачивание
        const ticketImage = document.querySelector('img[alt*="билет" i], img[alt*="ticket" i], .ticket-image img, .bilet img');
        const ticketLink = downloadButton.href || downloadButton.getAttribute('data-href') || window.location.href;
        
        // Отправляем сообщение в background script
        chrome.runtime.sendMessage({
          action: 'add_tickets_to_plan',
          ticket_url: ticketLink,
          ticket_image_url: ticketImage ? ticketImage.src : null,
          page_url: window.location.href
        }, (response) => {
          if (chrome.runtime.lastError) {
            console.error('Ошибка отправки сообщения:', chrome.runtime.lastError);
            alert('Ошибка: Расширение не подключено. Пожалуйста, проверьте подключение к боту.');
            return;
          }
          
          if (response && response.success) {
            if (response.message) {
              alert(response.message);
            } else {
              alert('✅ Для добавления билета:\n\n1. Скопируйте изображение билета (Ctrl+C или Cmd+C)\n2. Вставьте его в чат с ботом (Ctrl+V или Cmd+V)\n3. Бот автоматически распознает билет и добавит его к плану');
            }
          } else {
            const errorMsg = response && response.error ? response.error : 'Не удалось добавить билет';
            alert(`❌ ${errorMsg}\n\nУбедитесь, что вы:\n1. Привязали расширение через /code в боте\n2. Оформили подписку "Билеты"\n3. Запланировали просмотр фильма "В кино"`);
          }
        });
      } catch (error) {
        console.error('Ошибка при добавлении билета:', error);
        alert('Ошибка при добавлении билета. Попробуйте скопировать изображение билета и вставить в чат с ботом.');
      }
    });
    
    // Вставляем кнопку после кнопки скачивания
    downloadButton.parentNode.insertBefore(addTicketsBtn, downloadButton.nextSibling);
  }
  
  // Функция поиска кнопок скачивания билетов
  function findDownloadButtons() {
    // Различные селекторы для разных сайтов
    const selectors = [
      // Яндекс Афиша
      'a[href*="download"], a[href*="скачать"], button[aria-label*="скачать" i], button[aria-label*="download" i]',
      // Афиша.ру
      '.download-ticket, .download-btn, a.ticket-download',
      // Кинопоиск
      '.ticket-download, .download-bilet',
      // Общие паттерны
      'button:contains("Скачать"), a:contains("Скачать"), button:contains("Download"), a:contains("Download")',
      '[data-action="download"], [data-download]'
    ];
    
    const buttons = [];
    
    // Ищем по тексту (fallback)
    const allButtons = document.querySelectorAll('button, a');
    allButtons.forEach(btn => {
      const text = (btn.textContent || '').toLowerCase();
      if (text.includes('скачать') || text.includes('download') || 
          text.includes('билет') || text.includes('ticket')) {
        // Проверяем, что это не наша кнопка
        if (!btn.classList.contains('movieplanner-add-tickets-btn')) {
          buttons.push(btn);
        }
      }
    });
    
    return buttons;
  }
  
  // Функция инициализации
  function init() {
    // Ждем загрузки страницы
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => {
        setTimeout(addButtons, 1000); // Даем время на загрузку динамического контента
      });
    } else {
      setTimeout(addButtons, 1000);
    }
    
    // Наблюдаем за изменениями DOM (для динамического контента)
    const observer = new MutationObserver(() => {
      addButtons();
    });
    
    observer.observe(document.body, {
      childList: true,
      subtree: true
    });
  }
  
  // Функция добавления кнопок
  function addButtons() {
    const downloadButtons = findDownloadButtons();
    downloadButtons.forEach(btn => {
      createAddTicketsButton(btn);
    });
  }
  
  // Запускаем инициализацию
  init();
})();
