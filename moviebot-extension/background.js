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
  } else if (message.action === "open_ticket_upload") {
    // Не открываем popup автоматически - только при клике на иконку
    sendResponse({ success: true });
  }
  return true; // Для асинхронного ответа
});

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
