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
    chrome.action.openPopup();
    sendResponse({ success: true });
  } else if (message.action === "open_ticket_upload") {
    chrome.action.setPopup({ popup: `popup.html?ticket_url=${encodeURIComponent(message.url)}` });
    chrome.action.openPopup();
    sendResponse({ success: true });
  }
  return true; // Для асинхронного ответа
});

// Обработка найденного kp_id
async function handleKpId(kpId, isSeries, tab) {
  try {
    const data = await chrome.storage.local.get(['linked_chat_id']);
    if (!data.linked_chat_id) {
      return;
    }
    
    // Открываем popup с информацией о фильме
    chrome.action.setPopup({ popup: `popup.html?kp_id=${kpId}&is_series=${isSeries}` });
    chrome.action.openPopup();
  } catch (error) {
    console.error('Ошибка обработки kp_id:', error);
  }
}

// Обработка ссылки на фильм
async function handleLink(url, tab) {
  try {
    // Получаем chat_id из storage
    const data = await chrome.storage.local.get(['linked_chat_id']);
    if (!data.linked_chat_id) {
      chrome.tabs.create({ url: chrome.runtime.getURL('popup.html') });
      return;
    }
    
    // Открываем popup с информацией о фильме
    chrome.action.setPopup({ popup: 'popup.html?url=' + encodeURIComponent(url) });
    chrome.action.openPopup();
  } catch (error) {
    console.error('Ошибка обработки ссылки:', error);
  }
}

// Обработка найденного imdb_id
async function handleImdbId(imdbId, tab) {
  try {
    const data = await chrome.storage.local.get(['linked_chat_id']);
    if (!data.linked_chat_id) {
      return;
    }
    
    // Открываем popup с информацией о фильме
    chrome.action.setPopup({ popup: `popup.html?imdb_id=${imdbId}` });
    chrome.action.openPopup();
  } catch (error) {
    console.error('Ошибка обработки imdb_id:', error);
  }
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
