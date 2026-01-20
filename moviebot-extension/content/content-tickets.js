// content-tickets.js
// Content script для билетных сайтов: добавляет ОДНУ кнопку "Добавить билеты к плану" в правом верхнем углу экрана

(function() {
  'use strict';
  
  let floatingButton = null;
  
  // Функция поиска кнопок скачивания билетов
  function hasDownloadButtons() {
    // Ищем по тексту
    const allButtons = document.querySelectorAll('button, a');
    for (const btn of allButtons) {
      const text = (btn.textContent || '').toLowerCase();
      if (text.includes('скачать') || text.includes('download') || 
          text.includes('билет') || text.includes('ticket')) {
        // Проверяем, что это не наша кнопка
        if (!btn.classList.contains('movieplanner-add-tickets-btn')) {
          return true;
        }
      }
    }
    return false;
  }
  
  // Функция создания плавающей кнопки в правом верхнем углу
  function createFloatingButton() {
    // Удаляем существующую кнопку, если есть
    if (floatingButton) {
      floatingButton.remove();
    }
    
    // Проверяем, есть ли кнопки скачивания
    if (!hasDownloadButtons()) {
      return;
    }
    
    floatingButton = document.createElement('button');
    floatingButton.textContent = '🎟️ Добавить билеты к плану';
    floatingButton.className = 'movieplanner-add-tickets-btn';
    floatingButton.style.cssText = `
      position: fixed;
      top: 20px;
      right: 20px;
      z-index: 10000;
      padding: 12px 20px;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      border-radius: 8px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      transition: all 0.3s;
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    `;
    
    floatingButton.addEventListener('mouseenter', () => {
      floatingButton.style.transform = 'translateY(-2px)';
      floatingButton.style.boxShadow = '0 6px 16px rgba(102, 126, 234, 0.5)';
    });
    
    floatingButton.addEventListener('mouseleave', () => {
      floatingButton.style.transform = 'translateY(0)';
      floatingButton.style.boxShadow = '0 4px 12px rgba(102, 126, 234, 0.4)';
    });
    
    floatingButton.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      
      try {
        // Отправляем сообщение в background script
        chrome.runtime.sendMessage({
          action: 'add_tickets_to_plan',
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
    
    // Добавляем кнопку в body
    document.body.appendChild(floatingButton);
  }
  
  // Функция инициализации
  function init() {
    // Ждем загрузки страницы
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => {
        setTimeout(createFloatingButton, 1500); // Даем время на загрузку динамического контента
      });
    } else {
      setTimeout(createFloatingButton, 1500);
    }
    
    // Наблюдаем за изменениями DOM (для динамического контента)
    const observer = new MutationObserver(() => {
      // Проверяем, есть ли кнопки, и создаем/удаляем плавающую кнопку
      if (hasDownloadButtons()) {
        if (!floatingButton || !document.body.contains(floatingButton)) {
          createFloatingButton();
        }
      } else {
        if (floatingButton && document.body.contains(floatingButton)) {
          floatingButton.remove();
          floatingButton = null;
        }
      }
    });
    
    observer.observe(document.body, {
      childList: true,
      subtree: true
    });
  }
  
  // Запускаем инициализацию
  init();
})();
