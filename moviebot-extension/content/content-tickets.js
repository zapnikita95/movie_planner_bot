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
  
  // Функция загрузки позиции кнопки из localStorage
  function loadButtonPosition() {
    try {
      const saved = localStorage.getItem('movieplanner_button_position');
      if (saved) {
        const pos = JSON.parse(saved);
        return { top: pos.top || 20, right: pos.right || 20, left: pos.left, bottom: pos.bottom };
      }
    } catch (e) {
      console.error('Ошибка загрузки позиции кнопки:', e);
    }
    return { top: 20, right: 20 };
  }
  
  // Функция сохранения позиции кнопки в localStorage
  function saveButtonPosition(position) {
    try {
      localStorage.setItem('movieplanner_button_position', JSON.stringify(position));
    } catch (e) {
      console.error('Ошибка сохранения позиции кнопки:', e);
    }
  }
  
  // Функция создания плавающей кнопки в правом верхнем углу (перетаскиваемой)
  function createFloatingButton() {
    // Удаляем существующую кнопку, если есть
    if (floatingButton) {
      floatingButton.remove();
    }
    
    // Проверяем, есть ли кнопки скачивания или это страница расписания
    const isSchedulePage = window.location.href.includes('mos-kino.ru/schedule') || 
                          window.location.href.includes('mos-kino.ru') && document.querySelector('table, .schedule, [class*="schedule"]');
    
    if (!hasDownloadButtons() && !isSchedulePage) {
      return;
    }
    
    floatingButton = document.createElement('button');
    floatingButton.textContent = '🎟️ Добавить билеты к плану';
    floatingButton.className = 'movieplanner-add-tickets-btn';
    
    // Загружаем сохраненную позицию
    const savedPos = loadButtonPosition();
    const buttonStyle = `
      position: fixed;
      ${savedPos.left !== undefined ? `left: ${savedPos.left}px;` : ''}
      ${savedPos.right !== undefined ? `right: ${savedPos.right}px;` : ''}
      ${savedPos.top !== undefined ? `top: ${savedPos.top}px;` : ''}
      ${savedPos.bottom !== undefined ? `bottom: ${savedPos.bottom}px;` : ''}
      z-index: 10000;
      padding: 12px 20px;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      border-radius: 8px;
      cursor: move;
      font-size: 14px;
      font-weight: 500;
      transition: box-shadow 0.3s;
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
      user-select: none;
    `;
    floatingButton.style.cssText = buttonStyle;
    
    // Делаем кнопку перетаскиваемой
    let isDragging = false;
    let dragStartX = 0;
    let dragStartY = 0;
    let initialX = 0;
    let initialY = 0;
    let xOffset = 0;
    let yOffset = 0;
    let clickStartTime = 0;
    let hasMoved = false;
    
    // Инициализируем смещения из сохраненной позиции
    if (savedPos.left !== undefined) {
      xOffset = savedPos.left;
      floatingButton.style.left = `${savedPos.left}px`;
      floatingButton.style.right = 'auto';
    } else if (savedPos.right !== undefined) {
      floatingButton.style.right = `${savedPos.right}px`;
      floatingButton.style.left = 'auto';
    }
    if (savedPos.top !== undefined) {
      yOffset = savedPos.top;
      floatingButton.style.top = `${savedPos.top}px`;
      floatingButton.style.bottom = 'auto';
    } else if (savedPos.bottom !== undefined) {
      floatingButton.style.bottom = `${savedPos.bottom}px`;
      floatingButton.style.top = 'auto';
    }
    
    floatingButton.addEventListener('mousedown', (e) => {
      if (e.button !== 0) return; // Только левая кнопка мыши
      
      clickStartTime = Date.now();
      hasMoved = false;
      dragStartX = e.clientX;
      dragStartY = e.clientY;
      
      // Получаем текущую позицию кнопки
      const rect = floatingButton.getBoundingClientRect();
      initialX = rect.left;
      initialY = rect.top;
      xOffset = initialX;
      yOffset = initialY;
      
      e.preventDefault();
    });
    
    document.addEventListener('mousemove', (e) => {
      if (!clickStartTime) return;
      
      const deltaX = Math.abs(e.clientX - dragStartX);
      const deltaY = Math.abs(e.clientY - dragStartY);
      
      // Если мышь сдвинулась больше чем на 5px, начинаем перетаскивание
      if (deltaX > 5 || deltaY > 5) {
        hasMoved = true;
        if (!isDragging) {
          isDragging = true;
          floatingButton.style.cursor = 'grabbing';
          floatingButton.style.transition = 'none';
          floatingButton.style.userSelect = 'none';
        }
        
        currentX = e.clientX - dragStartX + initialX;
        currentY = e.clientY - dragStartY + initialY;
        
        // Ограничиваем перемещение границами экрана
        currentX = Math.max(0, Math.min(currentX, window.innerWidth - floatingButton.offsetWidth));
        currentY = Math.max(0, Math.min(currentY, window.innerHeight - floatingButton.offsetHeight));
        
        floatingButton.style.left = `${currentX}px`;
        floatingButton.style.top = `${currentY}px`;
        floatingButton.style.right = 'auto';
        floatingButton.style.bottom = 'auto';
        floatingButton.style.transform = 'none';
      }
    });
    
    document.addEventListener('mouseup', (e) => {
      if (isDragging) {
        isDragging = false;
        floatingButton.style.cursor = 'move';
        floatingButton.style.transition = 'box-shadow 0.3s';
        floatingButton.style.userSelect = 'none';
        
        // Сохраняем позицию
        const rect = floatingButton.getBoundingClientRect();
        const position = {
          left: rect.left,
          top: rect.top,
          right: window.innerWidth - rect.right,
          bottom: window.innerHeight - rect.bottom
        };
        saveButtonPosition(position);
      }
      
      clickStartTime = 0;
      hasMoved = false;
    });
    
    floatingButton.addEventListener('mouseenter', () => {
      if (!isDragging) {
        const currentTop = parseFloat(floatingButton.style.top) || 0;
        floatingButton.style.top = `${currentTop - 2}px`;
        floatingButton.style.boxShadow = '0 6px 16px rgba(102, 126, 234, 0.5)';
      }
    });
    
    floatingButton.addEventListener('mouseleave', () => {
      if (!isDragging) {
        const currentTop = parseFloat(floatingButton.style.top) || 0;
        floatingButton.style.top = `${currentTop + 2}px`;
        floatingButton.style.boxShadow = '0 4px 12px rgba(102, 126, 234, 0.4)';
      }
    });
    
    floatingButton.addEventListener('click', async (e) => {
      // Если это было перетаскивание, не обрабатываем клик
      if (hasMoved || isDragging) {
        return;
      }
      
      e.preventDefault();
      e.stopPropagation();
      
      try {
        // Открываем popup расширения с параметром для автоматического открытия формы планирования
        // Сохраняем флаг в storage, чтобы popup мог его прочитать
        chrome.storage.local.set({ auto_plan_cinema: true }, () => {
          // Открываем popup расширения
          chrome.runtime.sendMessage({
            action: 'open_popup_for_tickets'
          });
        });
      } catch (error) {
        console.error('Ошибка при открытии popup:', error);
        alert('Пожалуйста, откройте расширение вручную и выберите фильм для планирования.');
      }
    });
    
    // Добавляем кнопку в body
    document.body.appendChild(floatingButton);
  }
  
  // Функция инициализации
  function init() {
    // Проверяем, это ли страница mos-kino.ru/schedule
    const isMosKinoSchedule = window.location.href.includes('mos-kino.ru/schedule') || 
                              (window.location.href.includes('mos-kino.ru') && document.querySelector('table, .schedule, [class*="schedule"], [id*="schedule"]'));
    
    // Ждем загрузки страницы
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => {
        setTimeout(() => {
          if (isMosKinoSchedule || hasDownloadButtons()) {
            createFloatingButton();
          }
        }, 1500); // Даем время на загрузку динамического контента
      });
    } else {
      setTimeout(() => {
        if (isMosKinoSchedule || hasDownloadButtons()) {
          createFloatingButton();
        }
      }, 1500);
    }
    
    // Наблюдаем за изменениями DOM (для динамического контента)
    const observer = new MutationObserver(() => {
      // Проверяем, есть ли кнопки, или это страница расписания mos-kino
      const isSchedulePage = window.location.href.includes('mos-kino.ru/schedule') || 
                            (window.location.href.includes('mos-kino.ru') && document.querySelector('table, .schedule, [class*="schedule"], [id*="schedule"]'));
      
      if (hasDownloadButtons() || isSchedulePage) {
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
