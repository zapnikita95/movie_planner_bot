// background.js - Service Worker для расширения

const API_BASE_URL = 'https://web-production-3921c.up.railway.app';

// Создаём пункт меню при установке
chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.removeAll(() => {
    chrome.contextMenus.create({
      id: "send-to-moviebot",
      title: "Отправить в Movie Planner Bot",
      contexts: ["link"]
    });
  });
});

// Обработка клика по пункту меню
chrome.contextMenus.onClicked.addListener((info, tab) => {
  if (info.menuItemId === "send-to-moviebot" && info.linkUrl) {
    handleLink(info.linkUrl, tab);
  }
});

// Обработка сообщений от content scripts
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  try {
    if (message.action === "found_imdb_id") {
      handleImdbId(message.imdbId, sender.tab);
      sendResponse({ success: true });
    } else if (message.action === "found_kp_id") {
      handleKpId(message.kp_id, message.is_series, sender.tab);
      sendResponse({ success: true });
    } else if (message.action === "letterboxd_fallback") {
      // Fallback для letterboxd если не найден imdb_id
      sendResponse({ success: true });
    } else if (message.action === "found_ticket_site") {
      handleTicketSite(sender.tab);
      sendResponse({ success: true });
    } else if (message.action === "open_popup") {
      // Не открываем popup автоматически - только при клике на иконку
      sendResponse({ success: true });
    } else if (message.action === "open_popup_for_tickets") {
      // Открываем popup с параметром для автоматического открытия формы планирования
      chrome.action.openPopup();
      sendResponse({ success: true });
    } else if (message.action === "open_ticket_upload") {
      // Не открываем popup автоматически - только при клике на иконку
      sendResponse({ success: true });
    } else if (message.action === "add_tickets_to_plan") {
      // Обработка добавления билетов к плану
      handleAddTicketsToPlan(message, sender.tab, sendResponse);
      return true; // Асинхронный ответ
    } else if (message.action === "streaming_api_request") {
      // Обработка API запросов для streaming content script
      handleStreamingApiRequest(message, sendResponse);
      return true; // Асинхронный ответ
    }
  } catch (error) {
    console.error('Ошибка обработки сообщения:', error);
    // Пытаемся отправить ответ об ошибке, если это возможно
    try {
      sendResponse({ success: false, error: error.message });
    } catch (e) {
      // Игнорируем ошибку, если sendResponse уже был вызван
    }
  }
  return true; // Для асинхронного ответа
});

// Обработка добавления билетов к плану
async function handleAddTicketsToPlan(message, tab, sendResponse) {
  try {
    // Проверяем, привязан ли аккаунт
    const data = await chrome.storage.local.get(['linked_chat_id', 'has_tickets_access']);
    if (!data.linked_chat_id) {
      sendResponse({ 
        success: false, 
        error: 'Необходимо привязать аккаунт через /code в боте' 
      });
      return;
    }
    
    if (!data.has_tickets_access) {
      sendResponse({ 
        success: false, 
        error: 'Необходима подписка "Билеты" для добавления билетов' 
      });
      return;
    }
    
    // Отправляем билет в бота через API
    // Пока просто показываем инструкцию, так как нужен API endpoint для загрузки изображений
    // В будущем можно добавить API endpoint /api/extension/add-ticket для загрузки билетов
    
    sendResponse({ 
      success: true, 
      message: 'Для добавления билетов скопируйте изображение билета и вставьте его в чат с ботом. Бот автоматически распознает билет и добавит его к плану.' 
    });
    
    // Показываем уведомление
    chrome.notifications.create({
      type: 'basic',
      iconUrl: 'icons/icon48.png',
      title: 'Movie Planner Bot',
      message: 'Скопируйте изображение билета и вставьте его в чат с ботом'
    });
  } catch (error) {
    console.error('Ошибка обработки билета:', error);
    sendResponse({ success: false, error: error.message });
  }
}

// Обработка найденного kp_id
async function handleKpId(kpId, isSeries, tab) {
  // Не открываем popup автоматически - только при клике на иконку
  // Popup сам определит фильм из текущей вкладки
}

// Обработка ссылки на фильм
async function handleLink(url, tab) {
  // Не открываем popup автоматически - только при клике на иконку
  // Popup сам определит фильм из текущей вкладки
}

// Обработка найденного imdb_id
async function handleImdbId(imdbId, tab) {
  // Не открываем popup автоматически - только при клике на иконку
  // Popup сам определит фильм из текущей вкладки
}

// Обработка сайта с билетами
async function handleTicketSite(tab) {
  try {
    const data = await chrome.storage.local.get(['linked_chat_id']);
    if (!data.linked_chat_id) {
      return;
    }
    
    // Можно показать уведомление или открыть popup
    chrome.action.setPopup({ popup: 'popup.html?ticket_site=' + encodeURIComponent(tab.url) });
  } catch (error) {
    console.error('Ошибка обработки сайта с билетами:', error);
  }
}

// Обработка API запросов для streaming content script
async function handleStreamingApiRequest(message, sendResponse) {
  try {
    const { method, url, body, headers } = message;
    
    console.log('[BACKGROUND] Streaming API request:', { method, url });
    
    const fetchOptions = {
      method: method || 'GET',
      headers: headers || { 'Content-Type': 'application/json' }
    };
    
    if (body) {
      fetchOptions.body = typeof body === 'string' ? body : JSON.stringify(body);
    }
    
    const response = await fetch(url, fetchOptions);
    const responseData = await response.json();
    
    console.log('[BACKGROUND] Streaming API response:', { status: response.status, ok: response.ok });
    
    sendResponse({
      success: response.ok,
      status: response.status,
      data: responseData
    });
  } catch (error) {
    console.error('[BACKGROUND] Ошибка API запроса:', error);
    sendResponse({
      success: false,
      error: error.message
    });
  }
}
