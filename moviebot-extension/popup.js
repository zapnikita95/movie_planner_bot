// popup.js - Логика popup расширения

const API_BASE_URL = 'https://web-production-3921c.up.railway.app';

let chatId = null;
let userId = null;
let currentFilm = null;
let lastDetectedUrl = null; // Для отслеживания изменений URL
let isProcessing = false; // Флаг для защиты от двойных кликов

// Инициализация
document.addEventListener('DOMContentLoaded', async () => {
  // Проверяем авторизацию
  const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
  if (data.linked_chat_id) {
    chatId = data.linked_chat_id;
    userId = data.linked_user_id;
    showMainScreen();
    
    // Получаем текущую активную вкладку и автоматически загружаем фильм
    try {
      const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
      if (tabs && tabs[0] && tabs[0].url) {
        const currentUrl = tabs[0].url;
        // Всегда обновляем, даже если URL не изменился (на случай обновления страницы)
        lastDetectedUrl = currentUrl;
        await detectAndLoadFilm(currentUrl);
      }
    } catch (error) {
      console.error('Ошибка получения текущей вкладки:', error);
    }
    
    // Также проверяем параметры URL (для обратной совместимости)
    const urlParams = new URLSearchParams(window.location.search);
    const imdbId = urlParams.get('imdb_id');
    const kpId = urlParams.get('kp_id');
    const url = urlParams.get('url');
    const ticketUrl = urlParams.get('ticket_url');
    
    if (ticketUrl) {
      showTicketUpload(ticketUrl);
    } else if (kpId && !currentFilm) {
      await loadFilmByKpId(kpId);
    } else if (imdbId && !currentFilm) {
      await loadFilmByImdbId(imdbId);
    } else if (url && !currentFilm) {
      await loadFilmByUrl(url);
    }
  } else {
    showAuthScreen();
  }
  
  // Обработчики событий
  document.getElementById('bind-btn').addEventListener('click', handleBind);
  const logoutBtn = document.getElementById('logout-btn');
  logoutBtn.addEventListener('click', handleLogout);
  logoutBtn.title = 'Нажмите, чтобы отвязать аккаунт';
  document.getElementById('create-plan-btn').addEventListener('click', handleCreatePlan);
  document.getElementById('cancel-plan-btn').addEventListener('click', () => {
    document.getElementById('planning-form').classList.add('hidden');
  });
  
  // Обработчики для кнопок "Дома/В кино"
  document.getElementById('plan-type-home').addEventListener('click', () => {
    setPlanType('home');
  });
  document.getElementById('plan-type-cinema').addEventListener('click', () => {
    setPlanType('cinema');
  });
  
  // Убеждаемся, что форма планирования скрыта изначально
  document.getElementById('planning-form').classList.add('hidden');
});

// Автоматическое определение и загрузка фильма с текущей страницы
async function detectAndLoadFilm(url) {
  if (!url || !chatId) return;
  
  try {
    // Показываем индикатор загрузки
    document.getElementById('film-info').classList.remove('hidden');
    document.getElementById('film-title').textContent = 'Загружаем информацию о фильме';
    document.getElementById('film-year').textContent = '';
    document.getElementById('film-status').innerHTML = '';
    document.getElementById('film-actions').innerHTML = '';
    
    // Кинопоиск
    const kpMatch = url.match(/kinopoisk\.ru\/(film|series)\/(\d+)/i);
    if (kpMatch) {
      const kpId = kpMatch[2];
      // Пытаемся получить данные от content script, если нет - используем URL
      try {
        const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
        if (tabs && tabs[0]) {
          chrome.tabs.sendMessage(tabs[0].id, { action: 'get_kp_id' }, async (response) => {
            // Проверяем ошибки Chrome runtime
            if (chrome.runtime.lastError) {
              console.log('Content script не отвечает, используем URL:', chrome.runtime.lastError.message);
              await loadFilmByKpId(kpId);
              return;
            }
            if (response && response.kpId) {
              await loadFilmByKpId(response.kpId);
            } else {
              await loadFilmByKpId(kpId);
            }
          });
        } else {
          await loadFilmByKpId(kpId);
        }
      } catch (error) {
        console.error('Ошибка получения kp_id:', error);
        await loadFilmByKpId(kpId);
      }
      return;
    }
    
    // IMDb
    const imdbMatch = url.match(/imdb\.com\/title\/(tt\d+)/i);
    if (imdbMatch) {
      const imdbId = imdbMatch[1];
      // Пытаемся получить данные от content script
      try {
        const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
        if (tabs && tabs[0]) {
          chrome.tabs.sendMessage(tabs[0].id, { action: 'get_imdb_id' }, async (response) => {
            // Проверяем ошибки Chrome runtime
            if (chrome.runtime.lastError) {
              console.log('Content script не отвечает, используем URL:', chrome.runtime.lastError.message);
              await loadFilmByImdbId(imdbId);
              return;
            }
            if (response && response.imdbId) {
              await loadFilmByImdbId(response.imdbId);
            } else {
              await loadFilmByImdbId(imdbId);
            }
          });
        } else {
          await loadFilmByImdbId(imdbId);
        }
      } catch (error) {
        console.error('Ошибка получения imdb_id:', error);
        await loadFilmByImdbId(imdbId);
      }
      return;
    }
    
    // Letterboxd
    if (url.includes('letterboxd.com/film/')) {
      // Запрашиваем imdb_id у content script
      try {
        const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
        if (tabs && tabs[0]) {
          chrome.tabs.sendMessage(tabs[0].id, { action: 'get_imdb_id' }, async (response) => {
            // Проверяем ошибки Chrome runtime
            if (chrome.runtime.lastError) {
              console.log('Content script не отвечает:', chrome.runtime.lastError.message);
              document.getElementById('film-title').textContent = 'Не удалось определить фильм';
              document.getElementById('film-year').textContent = 'Попробуйте открыть страницу фильма на Кинопоиске или IMDb';
              return;
            }
            if (response && response.imdbId) {
              await loadFilmByImdbId(response.imdbId);
            } else {
              // Fallback: пытаемся загрузить через URL (но это не сработает для letterboxd)
              document.getElementById('film-title').textContent = 'Не удалось определить фильм';
              document.getElementById('film-year').textContent = 'Попробуйте открыть страницу фильма на Кинопоиске или IMDb';
            }
          });
        } else {
          document.getElementById('film-title').textContent = 'Не удалось определить фильм';
          document.getElementById('film-year').textContent = 'Попробуйте открыть страницу фильма на Кинопоиске или IMDb';
        }
      } catch (error) {
        console.error('Ошибка получения imdb_id:', error);
        document.getElementById('film-title').textContent = 'Ошибка загрузки';
        document.getElementById('film-year').textContent = 'Попробуйте обновить страницу';
      }
      return;
    }
    
    // Если не распознан - скрываем информацию о фильме
    document.getElementById('film-info').classList.add('hidden');
  } catch (error) {
    console.error('Ошибка определения фильма:', error);
    document.getElementById('film-info').classList.add('hidden');
  }
}

function showAuthScreen() {
  document.getElementById('auth-screen').classList.remove('hidden');
  document.getElementById('main-screen').classList.add('hidden');
}

function showMainScreen() {
  document.getElementById('auth-screen').classList.add('hidden');
  document.getElementById('main-screen').classList.remove('hidden');
}

async function handleBind() {
  const code = document.getElementById('code-input').value.trim().toUpperCase();
  const statusEl = document.getElementById('status');
  
  if (!code) {
    statusEl.textContent = 'Введите код';
    statusEl.className = 'status error';
    return;
  }
  
  statusEl.textContent = 'Проверяем...';
  statusEl.className = 'status';
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/verify?code=${code}`);
    const json = await response.json();
    
    if (json.success && json.chat_id) {
      await chrome.storage.local.set({ 
        linked_chat_id: json.chat_id,
        linked_user_id: json.user_id 
      });
      chatId = json.chat_id;
      userId = json.user_id;
      statusEl.textContent = '✅ Привязано!';
      statusEl.className = 'status success';
      setTimeout(async () => {
        showMainScreen();
        // После привязки автоматически загружаем фильм с текущей страницы
        try {
          const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
          if (tabs && tabs[0] && tabs[0].url) {
            await detectAndLoadFilm(tabs[0].url);
          }
        } catch (error) {
          console.error('Ошибка загрузки фильма после привязки:', error);
        }
      }, 1000);
    } else {
      statusEl.textContent = json.error || 'Неверный код';
      statusEl.className = 'status error';
    }
  } catch (err) {
    statusEl.textContent = 'Ошибка сети';
    statusEl.className = 'status error';
    console.error('Ошибка привязки:', err);
  }
}

async function handleLogout() {
  if (confirm('Отвязать аккаунт от браузера?')) {
    await chrome.storage.local.remove(['linked_chat_id', 'linked_user_id']);
    chatId = null;
    userId = null;
    showAuthScreen();
  }
}

async function loadFilmByImdbId(imdbId) {
  if (!imdbId || !chatId) return;
  
  try {
    // Показываем индикатор загрузки
    document.getElementById('film-info').classList.remove('hidden');
    document.getElementById('film-title').textContent = 'Загружаем информацию о фильме';
    document.getElementById('film-year').textContent = '';
    document.getElementById('film-status').innerHTML = '';
    document.getElementById('film-actions').innerHTML = '';
    
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?imdb_id=${imdbId}&chat_id=${chatId}`);
    const json = await response.json();
    
    if (json.success) {
      displayFilmInfo(json.film, json);
    } else {
      document.getElementById('film-title').textContent = 'Фильм не найден';
      document.getElementById('film-year').textContent = json.error || 'Попробуйте другую ссылку';
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    document.getElementById('film-info').classList.remove('hidden');
    document.getElementById('film-title').textContent = 'Ошибка загрузки';
    document.getElementById('film-year').textContent = 'Проверьте подключение к интернету';
  }
}

async function loadFilmByUrl(url) {
  // Парсим URL и извлекаем imdb_id или kp_id
  const imdbMatch = url.match(/imdb\.com\/title\/(tt\d+)/i);
  if (imdbMatch) {
    await loadFilmByImdbId(imdbMatch[1]);
    return;
  }
  
  const kpMatch = url.match(/kinopoisk\.ru\/(film|series)\/(\d+)/i);
  if (kpMatch) {
    await loadFilmByKpId(kpMatch[2]);
    return;
  }
  
  // Если не распознан URL, показываем ошибку
  alert('Не удалось распознать ссылку на фильм');
}

function displayFilmInfo(film, data) {
  console.log('[DISPLAY FILM] displayFilmInfo вызвана, film:', film, 'data:', data);
  
  // Очищаем предыдущее состояние
  currentFilm = null;
  
  // Устанавливаем новое состояние
  currentFilm = film;
  currentFilm.film_id = data.film_id;
  
  console.log('[DISPLAY FILM] currentFilm установлен:', currentFilm, 'kp_id:', currentFilm.kp_id);
  
  document.getElementById('film-title').textContent = film.title;
  document.getElementById('film-year').textContent = film.year || '';
  
  const statusEl = document.getElementById('film-status');
  statusEl.innerHTML = '';
  
  if (data.in_database) {
    statusEl.innerHTML += '<span class="badge in-db">В базе</span>';
  }
  if (data.watched) {
    statusEl.innerHTML += '<span class="badge watched">Просмотрено</span>';
  }
  if (data.has_plan) {
    statusEl.innerHTML += '<span class="badge has-plan">В расписании</span>';
  }
  
  const actionsEl = document.getElementById('film-actions');
  actionsEl.innerHTML = '';
  
  // Логика кнопок как в боте:
  // 1. Если фильм НЕ в базе - показываем "Добавить в базу" и "Запланировать просмотр"
  // 2. Если фильм в базе - показываем "Удалить из базы" и "Запланировать просмотр" (или "Изменить в расписании" если уже запланирован)
  
  // Всегда показываем две кнопки
  // Если фильм не в базе - "Добавить в базу", если в базе - "Удалить из базы"
  const dbBtn = document.createElement('button');
  if (!data.in_database) {
    dbBtn.textContent = '➕ Добавить в базу';
    dbBtn.className = 'btn btn-primary';
    dbBtn.addEventListener('click', async () => {
      if (isProcessing) return;
      console.log('[BUTTON CLICK] Клик по "Добавить в базу", film.kp_id:', film.kp_id);
      isProcessing = true;
      dbBtn.disabled = true;
      dbBtn.textContent = '⏳ Добавляем...';
      try {
        await addFilmToDatabase(film.kp_id);
      } finally {
        isProcessing = false;
      }
    });
  } else {
    dbBtn.textContent = '🗑️ Удалить из базы';
    dbBtn.className = 'btn btn-secondary';
    dbBtn.addEventListener('click', async () => {
      if (isProcessing) return;
      if (confirm('Вы уверены, что хотите удалить фильм из базы?')) {
        isProcessing = true;
        dbBtn.disabled = true;
        dbBtn.textContent = '⏳ Удаляем...';
        try {
          await deleteFilmFromDatabase(film.kp_id);
        } finally {
          isProcessing = false;
        }
      }
    });
  }
  actionsEl.appendChild(dbBtn);
  
  // Кнопка "Запланировать просмотр"
  const planBtn = document.createElement('button');
  planBtn.textContent = data.has_plan ? '✏️ Изменить в расписании' : '📅 Запланировать просмотр';
  planBtn.className = 'btn btn-primary';
  planBtn.addEventListener('click', () => {
    if (isProcessing) return;
    showPlanningForm();
  });
  actionsEl.appendChild(planBtn);
  
  document.getElementById('film-info').classList.remove('hidden');
}

async function addFilmToDatabase(kpId) {
  console.log('[ADD FILM] Начало функции, kpId:', kpId, 'chatId:', chatId);
  
  if (!kpId) {
    console.error('[ADD FILM] Ошибка: kpId не указан');
    alert('Ошибка: не указан ID фильма');
    isProcessing = false;
    return;
  }
  
  if (!chatId) {
    console.error('[ADD FILM] Ошибка: chatId не указан');
    alert('Ошибка: не авторизован. Пожалуйста, привяжите аккаунт через /code в боте');
    isProcessing = false;
    return;
  }
  
  try {
    const url = `${API_BASE_URL}/api/extension/add-film`;
    const body = JSON.stringify({ kp_id: kpId, chat_id: chatId });
    
    console.log('[ADD FILM] Отправка запроса:', { url, body, kp_id: kpId, chat_id: chatId });
    
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body
    });
    
    console.log('[ADD FILM] Получен ответ:', { status: response.status, statusText: response.statusText, ok: response.ok });
    
    if (!response.ok) {
      // Пытаемся получить текст ошибки
      let errorText = '';
      try {
        const errorJson = await response.json();
        errorText = errorJson.error || 'неизвестная ошибка';
        console.error('[ADD FILM] Ошибка от сервера:', errorJson);
      } catch (e) {
        errorText = await response.text();
        console.error('[ADD FILM] Ошибка парсинга ответа:', errorText);
      }
      throw new Error(`HTTP error! status: ${response.status}, error: ${errorText}`);
    }
    
    const json = await response.json();
    console.log('[ADD FILM] Ответ сервера:', json);
    
    if (json.success) {
      if (currentFilm) {
        currentFilm.film_id = json.film_id;
      }
      // Показываем сообщение
      const statusEl = document.getElementById('status');
      if (statusEl) {
        statusEl.textContent = '✅ Добавлено в базу!';
        statusEl.className = 'status success';
        setTimeout(() => {
          statusEl.textContent = '';
          statusEl.className = 'status';
        }, 3000);
      }
      // Перезагружаем информацию по kp_id
      if (currentFilm && currentFilm.kp_id) {
        await loadFilmByKpId(currentFilm.kp_id);
      } else if (currentFilm && currentFilm.imdb_id) {
        await loadFilmByImdbId(currentFilm.imdb_id);
      } else {
        // Если currentFilm не установлен, загружаем по kpId
        await loadFilmByKpId(kpId);
      }
    } else {
      alert('Ошибка добавления фильма: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('[ADD FILM] Ошибка в catch блоке:', err);
    console.error('[ADD FILM] Stack trace:', err.stack);
    const errorMessage = err.message || 'Проверьте подключение к интернету';
    console.error('[ADD FILM] Показываем alert с ошибкой:', errorMessage);
    alert('Ошибка добавления фильма: ' + errorMessage);
  } finally {
    isProcessing = false;
  }
}

async function deleteFilmFromDatabase(kpId) {
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/delete-film`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kp_id: kpId, chat_id: chatId })
    });
    
    if (!response.ok) {
      let errorText = '';
      try {
        const errorJson = await response.json();
        errorText = errorJson.error || 'неизвестная ошибка';
      } catch (e) {
        errorText = await response.text();
      }
      throw new Error(`HTTP error! status: ${response.status}, error: ${errorText}`);
    }
    
    const json = await response.json();
    if (json.success) {
      // Перезагружаем информацию
      if (currentFilm && currentFilm.kp_id) {
        await loadFilmByKpId(currentFilm.kp_id);
      } else if (currentFilm && currentFilm.imdb_id) {
        await loadFilmByImdbId(currentFilm.imdb_id);
      }
    } else {
      alert('Ошибка удаления фильма: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('Ошибка удаления фильма:', err);
    alert('Ошибка удаления фильма: ' + (err.message || 'Проверьте подключение к интернету'));
  } finally {
    isProcessing = false;
  }
}

async function loadFilmByKpId(kpId) {
  if (!kpId || !chatId) return;
  
  try {
    // Показываем индикатор загрузки
    document.getElementById('film-info').classList.remove('hidden');
    document.getElementById('film-title').textContent = 'Загружаем информацию о фильме';
    document.getElementById('film-year').textContent = '';
    document.getElementById('film-status').innerHTML = '';
    document.getElementById('film-actions').innerHTML = '';
    
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${chatId}`);
    const json = await response.json();
    
    if (json.success) {
      displayFilmInfo(json.film, json);
    } else {
      document.getElementById('film-title').textContent = 'Фильм не найден';
      document.getElementById('film-year').textContent = json.error || 'Попробуйте другую ссылку';
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    document.getElementById('film-info').classList.remove('hidden');
    document.getElementById('film-title').textContent = 'Ошибка загрузки';
    document.getElementById('film-year').textContent = 'Проверьте подключение к интернету';
  }
}

function showPlanningForm() {
  // Если фильм не в базе, сначала добавляем его
  if (!currentFilm || !currentFilm.film_id) {
    if (currentFilm && currentFilm.kp_id) {
      // Автоматически добавляем в базу, если еще не добавлен
      addFilmToDatabase(currentFilm.kp_id).then(() => {
        // После добавления показываем форму
        if (currentFilm && currentFilm.film_id) {
          document.getElementById('planning-form').classList.remove('hidden');
          initializePlanningForm();
        }
      });
      return;
    } else {
      alert('Сначала добавьте фильм в базу');
      return;
    }
  }
  
  document.getElementById('planning-form').classList.remove('hidden');
  initializePlanningForm();
}

function initializePlanningForm() {
  // Сбрасываем выбор типа плана на "Дома"
  setPlanType('home');
  
  // Устанавливаем минимальную дату (сегодня) и предустанавливаем текущий год
  const now = new Date();
  now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
  document.getElementById('plan-datetime').min = now.toISOString().slice(0, 16);
  
  // Предустанавливаем текущий год (если не декабрь)
  const currentMonth = now.getMonth() + 1; // 1-12
  if (currentMonth !== 12) {
    // Устанавливаем дату на сегодня с текущим годом
    const defaultDate = new Date(now);
    defaultDate.setHours(19, 0, 0, 0); // 19:00 по умолчанию
    document.getElementById('plan-datetime').value = defaultDate.toISOString().slice(0, 16);
  }
  
  // Очищаем поле текстового времени
  document.getElementById('plan-time-text').value = '';
}

let selectedPlanType = 'home'; // По умолчанию "Дома"

function setPlanType(type) {
  selectedPlanType = type;
  
  // Обновляем классы кнопок
  const homeBtn = document.getElementById('plan-type-home');
  const cinemaBtn = document.getElementById('plan-type-cinema');
  const streamingEl = document.getElementById('streaming-services');
  
  if (type === 'home') {
    homeBtn.classList.remove('btn-secondary');
    homeBtn.classList.add('btn-primary', 'active');
    homeBtn.style.border = '2px solid #007bff';
    cinemaBtn.classList.remove('btn-primary', 'active');
    cinemaBtn.classList.add('btn-secondary');
    cinemaBtn.style.border = '2px solid transparent';
    streamingEl.classList.remove('hidden');
    // Загружаем список онлайн-кинотеатров из API
    if (currentFilm && currentFilm.kp_id) {
      loadStreamingServices(currentFilm.kp_id);
    }
  } else {
    cinemaBtn.classList.remove('btn-secondary');
    cinemaBtn.classList.add('btn-primary', 'active');
    cinemaBtn.style.border = '2px solid #007bff';
    homeBtn.classList.remove('btn-primary', 'active');
    homeBtn.classList.add('btn-secondary');
    homeBtn.style.border = '2px solid transparent';
    streamingEl.classList.add('hidden');
  }
}

async function loadStreamingServices(kpId) {
  if (!kpId) return;
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/streaming-services?kp_id=${kpId}`);
    if (!response.ok) {
      console.error('Ошибка загрузки стриминговых сервисов:', response.status);
      return;
    }
    
    const json = await response.json();
    const select = document.getElementById('streaming-service');
    
    // Очищаем текущие опции (кроме первой "Выберите сервис")
    select.innerHTML = '<option value="">Выберите сервис</option>';
    
    if (json.success && json.services && json.services.length > 0) {
      json.services.forEach(service => {
        const option = document.createElement('option');
        option.value = service.name;
        option.textContent = service.name;
        select.appendChild(option);
      });
    }
  } catch (err) {
    console.error('Ошибка загрузки стриминговых сервисов:', err);
  }
}

async function handleCreatePlan() {
  if (isProcessing) return;
  
  if (!currentFilm || !currentFilm.film_id) {
    alert('Фильм не выбран');
    return;
  }
  
  isProcessing = true;
  const createBtn = document.getElementById('create-plan-btn');
  const originalText = createBtn.textContent;
  createBtn.disabled = true;
  createBtn.textContent = '⏳ Создаём план...';
  
  try {
    const planType = selectedPlanType;
  const planTimeText = document.getElementById('plan-time-text').value.trim();
  const planDatetime = document.getElementById('plan-datetime').value;
  const streamingService = document.getElementById('streaming-service').value;
  
  let planDatetimeISO = null;
  
  // Если указано текстовое время - парсим его
  if (planTimeText) {
    try {
      const response = await fetch(`${API_BASE_URL}/api/extension/parse-time`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          time_text: planTimeText,
          user_id: userId
        })
      });
      
      if (!response.ok) {
        let errorText = '';
        try {
          const errorJson = await response.json();
          errorText = errorJson.error || 'неизвестная ошибка';
        } catch (e) {
          errorText = await response.text();
        }
        throw new Error(`HTTP error! status: ${response.status}, error: ${errorText}`);
      }
      
      const json = await response.json();
      if (json.success && json.datetime) {
        planDatetimeISO = json.datetime;
      } else {
        alert('Не удалось распознать время: ' + (json.error || 'неизвестная ошибка'));
        return;
      }
    } catch (err) {
      console.error('Ошибка парсинга времени:', err);
      alert('Ошибка парсинга времени: ' + (err.message || 'Проверьте подключение к интернету'));
      return;
    }
  } else if (planDatetime) {
    planDatetimeISO = new Date(planDatetime).toISOString();
  } else {
    alert('Укажите дату и время');
    return;
  }
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/create-plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        film_id: currentFilm.film_id,
        plan_type: planType,
        plan_datetime: planDatetimeISO,
        user_id: userId,
        streaming_service: streamingService || null,
        streaming_url: null
      })
    });
    
    if (!response.ok) {
      let errorText = '';
      try {
        const errorJson = await response.json();
        errorText = errorJson.error || 'неизвестная ошибка';
      } catch (e) {
        errorText = await response.text();
      }
      throw new Error(`HTTP error! status: ${response.status}, error: ${errorText}`);
    }
    
    const json = await response.json();
    if (json.success) {
      alert('✅ План создан!');
      document.getElementById('planning-form').classList.add('hidden');
      // Перезагружаем информацию о фильме
      if (currentFilm.kp_id) {
        await loadFilmByKpId(currentFilm.kp_id);
      } else if (currentFilm.imdb_id) {
        await loadFilmByImdbId(currentFilm.imdb_id);
      }
    } else {
      alert('Ошибка создания плана: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('Ошибка создания плана:', err);
    alert('Ошибка создания плана: ' + (err.message || 'Проверьте подключение к интернету'));
  } finally {
    isProcessing = false;
    createBtn.disabled = false;
    createBtn.textContent = originalText;
  }
}

function showTicketUpload(url) {
  // Показываем форму для загрузки билета
  const container = document.querySelector('.container');
  container.innerHTML = `
    <h1>Загрузка билета</h1>
    <p>Ссылка: <a href="${url}" target="_blank">${url}</a></p>
    <p class="subtitle">Билеты можно загружать через бота. Откройте бота и отправьте скриншот билета.</p>
    <button id="back-btn" class="btn btn-secondary">Назад</button>
  `;
  
  document.getElementById('back-btn').addEventListener('click', () => {
    window.location.href = 'popup.html';
  });
}
