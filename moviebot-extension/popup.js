// popup.js - Логика popup расширения

const API_BASE_URL = 'https://web-production-3921c.up.railway.app';

let chatId = null;
let userId = null;
let currentFilm = null;

// Инициализация
document.addEventListener('DOMContentLoaded', async () => {
  // Проверяем авторизацию
  const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
  if (data.linked_chat_id) {
    chatId = data.linked_chat_id;
    userId = data.linked_user_id;
    showMainScreen();
    
    // Проверяем параметры URL
    const urlParams = new URLSearchParams(window.location.search);
    const imdbId = urlParams.get('imdb_id');
    const kpId = urlParams.get('kp_id');
    const url = urlParams.get('url');
    const ticketUrl = urlParams.get('ticket_url');
    
    if (ticketUrl) {
      showTicketUpload(ticketUrl);
    } else if (kpId) {
      await loadFilmByKpId(kpId);
    } else if (imdbId) {
      await loadFilmByImdbId(imdbId);
    } else if (url) {
      await loadFilmByUrl(url);
    }
  } else {
    showAuthScreen();
  }
  
  // Обработчики событий
  document.getElementById('bind-btn').addEventListener('click', handleBind);
  document.getElementById('logout-btn').addEventListener('click', handleLogout);
  document.getElementById('create-plan-btn').addEventListener('click', handleCreatePlan);
  document.getElementById('plan-type').addEventListener('change', handlePlanTypeChange);
});

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
      setTimeout(() => {
        showMainScreen();
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
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?imdb_id=${imdbId}&chat_id=${chatId}`);
    const json = await response.json();
    
    if (json.success) {
      displayFilmInfo(json.film, json);
    } else {
      alert('Фильм не найден');
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    alert('Ошибка загрузки фильма');
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
  
  if (!data.in_database) {
    const addBtn = document.createElement('button');
    addBtn.textContent = '➕ Добавить в базу';
    addBtn.className = 'btn btn-primary';
    addBtn.addEventListener('click', () => addFilmToDatabase(film.kp_id));
    actionsEl.appendChild(addBtn);
  }
  
  const planBtn = document.createElement('button');
  planBtn.textContent = '📅 Запланировать просмотр';
  planBtn.className = 'btn btn-primary';
  planBtn.addEventListener('click', () => showPlanningForm());
  actionsEl.appendChild(planBtn);
  
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
      alert('✅ Фильм добавлен в базу!');
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

async function loadFilmByKpId(kpId) {
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${chatId}`);
    const json = await response.json();
    
    if (json.success) {
      displayFilmInfo(json.film, json);
    } else {
      alert('Фильм не найден');
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    alert('Ошибка загрузки фильма');
  }
}

function showPlanningForm() {
  if (!currentFilm || !currentFilm.film_id) {
    alert('Сначала добавьте фильм в базу');
    return;
  }
  
  document.getElementById('planning-form').classList.remove('hidden');
  
  // Устанавливаем минимальную дату (сегодня)
  const now = new Date();
  now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
  document.getElementById('plan-datetime').min = now.toISOString().slice(0, 16);
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
  const planDatetime = document.getElementById('plan-datetime').value;
  const streamingService = document.getElementById('streaming-service').value;
  
  if (!planDatetime) {
    alert('Выберите дату и время');
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
        plan_datetime: new Date(planDatetime).toISOString(),
        user_id: userId,
        streaming_service: streamingService || null,
        streaming_url: null
      })
    });
    
    const json = await response.json();
    if (json.success) {
      alert('✅ План создан!');
      document.getElementById('planning-form').classList.add('hidden');
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
