// popup.js - Логика popup расширения

const API_BASE_URL = 'https://web-production-3921c.up.railway.app';

let chatId = null;
let userId = null;
let currentFilm = null;
let lastDetectedUrl = null; // Для отслеживания изменений URL

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
  document.getElementById('logout-btn').addEventListener('click', handleLogout);
  document.getElementById('create-plan-btn').addEventListener('click', handleCreatePlan);
  document.getElementById('cancel-plan-btn').addEventListener('click', () => {
    document.getElementById('planning-form').classList.add('hidden');
  });
  document.getElementById('plan-type').addEventListener('change', handlePlanTypeChange);
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
            if (response && response.imdbId) {
              await loadFilmByImdbId(response.imdbId);
            } else {
              // Fallback: пытаемся загрузить через URL (но это не сработает для letterboxd)
              document.getElementById('film-title').textContent = 'Не удалось определить фильм';
              document.getElementById('film-year').textContent = 'Попробуйте открыть страницу фильма на Кинопоиске или IMDb';
            }
          });
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
  await chrome.storage.local.remove(['linked_chat_id', 'linked_user_id']);
  chatId = null;
  userId = null;
  showAuthScreen();
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
  // Очищаем предыдущее состояние
  currentFilm = null;
  
  // Устанавливаем новое состояние
  currentFilm = film;
  currentFilm.film_id = data.film_id;
  
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
  
  if (!data.in_database) {
    // Фильм не в базе - две кнопки
    const addBtn = document.createElement('button');
    addBtn.textContent = '➕ Добавить в базу';
    addBtn.className = 'btn btn-primary';
    addBtn.addEventListener('click', async () => {
      await addFilmToDatabase(film.kp_id);
    });
    actionsEl.appendChild(addBtn);
    
    const planBtn = document.createElement('button');
    planBtn.textContent = '📅 Запланировать просмотр';
    planBtn.className = 'btn btn-primary';
    planBtn.addEventListener('click', async () => {
      // Автоматически добавляем в базу, если еще не добавлен
      if (!data.in_database) {
        await addFilmToDatabase(film.kp_id);
      }
      showPlanningForm();
    });
    actionsEl.appendChild(planBtn);
  } else {
    // Фильм в базе
    if (data.has_plan) {
      // Просмотр уже запланирован - кнопка "Изменить в расписании"
      const editPlanBtn = document.createElement('button');
      editPlanBtn.textContent = '✏️ Изменить в расписании';
      editPlanBtn.className = 'btn btn-primary';
      editPlanBtn.addEventListener('click', () => showPlanningForm());
      actionsEl.appendChild(editPlanBtn);
    } else {
      // Просмотр не запланирован - кнопка "Запланировать просмотр"
      const planBtn = document.createElement('button');
      planBtn.textContent = '📅 Запланировать просмотр';
      planBtn.className = 'btn btn-primary';
      planBtn.addEventListener('click', () => showPlanningForm());
      actionsEl.appendChild(planBtn);
    }
    
    // Кнопка "Удалить из базы"
    const deleteBtn = document.createElement('button');
    deleteBtn.textContent = '🗑️ Удалить из базы';
    deleteBtn.className = 'btn btn-secondary';
    deleteBtn.addEventListener('click', async () => {
      if (confirm('Вы уверены, что хотите удалить фильм из базы?')) {
        await deleteFilmFromDatabase(film.kp_id);
      }
    });
    actionsEl.appendChild(deleteBtn);
  }
  
  document.getElementById('film-info').classList.remove('hidden');
}

async function addFilmToDatabase(kpId) {
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/add-film`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kp_id: kpId, chat_id: chatId })
    });
    
    const json = await response.json();
    if (json.success) {
      currentFilm.film_id = json.film_id;
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
      if (currentFilm.kp_id) {
        await loadFilmByKpId(currentFilm.kp_id);
      } else if (currentFilm.imdb_id) {
        await loadFilmByImdbId(currentFilm.imdb_id);
      }
    } else {
      alert('Ошибка добавления фильма: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('Ошибка добавления фильма:', err);
    alert('Ошибка добавления фильма');
  }
}

async function deleteFilmFromDatabase(kpId) {
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/delete-film`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kp_id: kpId, chat_id: chatId })
    });
    
    const json = await response.json();
    if (json.success) {
      // Перезагружаем информацию
      if (currentFilm.kp_id) {
        await loadFilmByKpId(currentFilm.kp_id);
      } else if (currentFilm.imdb_id) {
        await loadFilmByImdbId(currentFilm.imdb_id);
      }
    } else {
      alert('Ошибка удаления фильма: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('Ошибка удаления фильма:', err);
    alert('Ошибка удаления фильма');
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
  if (!currentFilm || !currentFilm.film_id) {
    alert('Сначала добавьте фильм в базу');
    return;
  }
  
  document.getElementById('planning-form').classList.remove('hidden');
  
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

function handlePlanTypeChange() {
  const planType = document.getElementById('plan-type').value;
  const streamingEl = document.getElementById('streaming-services');
  
  if (planType === 'home') {
    streamingEl.classList.remove('hidden');
    // TODO: загрузить список онлайн-кинотеатров из API
  } else {
    streamingEl.classList.add('hidden');
  }
}

async function handleCreatePlan() {
  if (!currentFilm || !currentFilm.film_id) {
    alert('Фильм не выбран');
    return;
  }
  
  const planType = document.getElementById('plan-type').value;
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
      
      const json = await response.json();
      if (json.success && json.datetime) {
        planDatetimeISO = json.datetime;
      } else {
        alert('Не удалось распознать время: ' + (json.error || 'неизвестная ошибка'));
        return;
      }
    } catch (err) {
      console.error('Ошибка парсинга времени:', err);
      alert('Ошибка парсинга времени');
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
    alert('Ошибка создания плана');
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
