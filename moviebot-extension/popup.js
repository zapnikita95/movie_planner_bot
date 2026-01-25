// popup.js - Логика popup расширения

const API_BASE_URL = 'https://web-production-3921c.up.railway.app';

function streamingApiRequest(method, url, body = null) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage({
      action: 'streaming_api_request',
      method: method || 'GET',
      url,
      headers: { 'Content-Type': 'application/json' },
      body: body ?? null
    }, (r) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      if (!r) {
        reject(new Error('Нет ответа от расширения'));
        return;
      }
      resolve({ ok: r.status >= 200 && r.status < 300, status: r.status, data: r.data || {}, error: r.error });
    });
  });
}

let chatId = null;
let userId = null;
let currentFilm = null;
let lastDetectedUrl = null; // Для отслеживания изменений URL
let isProcessing = false; // Флаг для защиты от двойных кликов
let urlRequestHistory = []; // История запросов для защиты от спама

// Функция сброса состояния расширения
function resetExtensionState() {
  currentFilm = null;
  isProcessing = false;
  
  // Скрываем форму планирования (ОБЯЗАТЕЛЬНО!) с !important
  const planningForm = document.getElementById('planning-form');
  if (planningForm) {
    planningForm.classList.add('hidden');
    planningForm.style.display = 'none';
  }
  
  // Очищаем информацию о фильме
  const filmInfo = document.getElementById('film-info');
  if (filmInfo) {
    filmInfo.classList.add('hidden');
    filmInfo.style.display = 'none';
    const titleEl = document.getElementById('film-title');
    const yearEl = document.getElementById('film-year');
    const statusEl = document.getElementById('film-status');
    const actionsEl = document.getElementById('film-actions');
    if (titleEl) titleEl.textContent = '';
    if (yearEl) yearEl.textContent = '';
    if (statusEl) statusEl.innerHTML = '';
    if (actionsEl) actionsEl.innerHTML = '';
  }
  
  const searchResults = document.getElementById('search-results');
  if (searchResults) searchResults.classList.add('hidden');
  
  const searchSection = document.getElementById('search-section');
  if (searchSection) {
    searchSection.classList.add('hidden');
    searchSection.style.display = 'none';
  }
  
  const streamingMarkLast = document.getElementById('streaming-mark-last');
  if (streamingMarkLast) {
    streamingMarkLast.classList.add('hidden');
    streamingMarkLast.style.display = 'none';
  }
  
  // Очищаем поле поиска
  const searchInput = document.getElementById('search-input');
  if (searchInput) searchInput.value = '';
}

// Инициализация
document.addEventListener('DOMContentLoaded', async () => {
  console.log('[POPUP INIT] DOMContentLoaded запущен');
  
  
  // Проверяем начальное состояние элементов
  const initCheck = {
    authScreen: document.getElementById('auth-screen')?.classList?.toString(),
    mainScreen: document.getElementById('main-screen')?.classList?.toString(),
    filmInfo: document.getElementById('film-info')?.classList?.toString()
  };
  console.log('[POPUP INIT] Начальное состояние элементов:', initCheck);
  
  // Скрываем блок подтверждения при инициализации
  const confirmationEl = document.getElementById('film-confirmation');
  if (confirmationEl) {
    confirmationEl.classList.add('hidden');
    confirmationEl.style.display = 'none';
  }
  
  // Устанавливаем правильные пути к логотипам
  const logoImg = document.getElementById('logo-img');
  const logoImgAuth = document.getElementById('logo-img-auth');
  if (logoImg) logoImg.src = chrome.runtime.getURL('icons/icon48.png');
  if (logoImgAuth) logoImgAuth.src = chrome.runtime.getURL('icons/icon48.png');
  
  // Проверяем авторизацию
  const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id', 'has_tickets_access']);
  if (data.linked_chat_id) {
    chatId = data.linked_chat_id;
    userId = data.linked_user_id;
    // Восстанавливаем статус подписки из кэша
    hasTicketsAccess = data.has_tickets_access || false;
    showMainScreen();
    
    // Проверяем подписку один раз при подключении
    const ticketsAccess = await checkTicketsSubscription();
    await chrome.storage.local.set({ has_tickets_access: ticketsAccess });
    hasTicketsAccess = ticketsAccess;
    
    // Получаем текущую активную вкладку и автоматически загружаем фильм
    // ВАЖНО: Всегда получаем свежий URL при каждом открытии popup (для SPA)
    await loadCurrentTabFilm();
    
    // Также проверяем параметры URL (для обратной совместимости)
    const urlParams = new URLSearchParams(window.location.search);
    const imdbId = urlParams.get('imdb_id');
    const kpId = urlParams.get('kp_id');
    const url = urlParams.get('url');
    const ticketUrl = urlParams.get('ticket_url');
    const autoPlanCinema = urlParams.get('auto_plan_cinema') === 'true';
    
    // Проверяем флаг auto_plan_cinema из storage (устанавливается из content script)
    const storageData = await chrome.storage.local.get(['auto_plan_cinema']);
    const shouldAutoPlanCinema = autoPlanCinema || storageData.auto_plan_cinema;
    
    // Если открыт для добавления билетов, показываем поиск
    if (shouldAutoPlanCinema) {
      // Удаляем флаг из storage
      chrome.storage.local.remove(['auto_plan_cinema']);
      const searchSection = document.getElementById('search-section');
      if (searchSection) {
        searchSection.classList.remove('hidden');
        searchSection.style.display = '';
      }
      // Сохраняем флаг для автоматического открытия формы планирования после выбора фильма
      window.autoPlanCinemaMode = true;
    }
    
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
  const bindBtn = document.getElementById('bind-btn');
  if (bindBtn) bindBtn.addEventListener('click', handleBind);
  
  const logoutLink = document.getElementById('logout-link');
  if (logoutLink) {
    logoutLink.addEventListener('click', handleLogout);
  }
  
  const createPlanBtn = document.getElementById('create-plan-btn');
  if (createPlanBtn) createPlanBtn.addEventListener('click', handleCreatePlan);
  
  const cancelPlanBtn = document.getElementById('cancel-plan-btn');
  if (cancelPlanBtn) {
    cancelPlanBtn.addEventListener('click', () => {
      const planningForm = document.getElementById('planning-form');
      if (planningForm) {
        planningForm.classList.add('hidden');
        planningForm.style.display = 'none';
      }
      // Завершаем процесс работы с фильмом - очищаем состояние
      resetExtensionState();
    });
  }
  
  // Обработчики для кнопок "Дома/В кино" (могут быть в скрытой форме)
  const planTypeHome = document.getElementById('plan-type-home');
  if (planTypeHome) {
    planTypeHome.addEventListener('click', () => {
      setPlanType('home');
    });
  }
  
  const planTypeCinema = document.getElementById('plan-type-cinema');
  if (planTypeCinema) {
    planTypeCinema.addEventListener('click', () => {
      setPlanType('cinema');
    });
  }
  
  // ОБЯЗАТЕЛЬНО скрываем форму планирования изначально (с !important стилем)
  const planningForm = document.getElementById('planning-form');
  if (planningForm) {
    planningForm.classList.add('hidden');
    planningForm.style.display = 'none';
  }
  
  // ОБЯЗАТЕЛЬНО скрываем поиск изначально (с !important стилем)
  const searchSection = document.getElementById('search-section');
  if (searchSection) {
    searchSection.classList.add('hidden');
    searchSection.style.display = 'none';
  }
  
  // Очищаем состояние при каждом открытии
  resetExtensionState();
  
  // Обработчик поиска (добавляем обработчики для поиска)
  const searchBtn = document.getElementById('search-btn');
  const searchInput = document.getElementById('search-input');
  if (searchBtn && searchInput) {
    searchBtn.addEventListener('click', () => performSearch());
    searchInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        performSearch();
      }
    });
  }
  
  // Обработчик галочки календаря
  const calendarCheckbox = document.getElementById('use-calendar-checkbox');
  const planDatetime = document.getElementById('plan-datetime');
  const planTimeText = document.getElementById('plan-time-text');
  if (calendarCheckbox && planDatetime && planTimeText) {
    calendarCheckbox.addEventListener('change', (e) => {
      if (e.target.checked) {
        // Галочка включена - календарь активен, текстовое поле неактивно
        planDatetime.disabled = false;
        planDatetime.style.backgroundColor = '';
        planTimeText.disabled = true;
        planTimeText.style.backgroundColor = '#f0f0f0';
        planTimeText.value = ''; // Очищаем текстовое поле
      } else {
        // Галочка выключена - текстовое поле активно, календарь неактивен
        planDatetime.disabled = true;
        planDatetime.style.backgroundColor = '#f0f0f0';
        planDatetime.value = ''; // Очищаем календарь
        planTimeText.disabled = false;
        planTimeText.style.backgroundColor = '';
      }
    });
  }
  
  // Обработчик кнопки добавления билетов
  const addTicketsBtn = document.getElementById('add-tickets-btn');
  if (addTicketsBtn) {
    addTicketsBtn.addEventListener('click', () => {
      if (!addTicketsBtn.disabled) {
        alert('🎟️ Для добавления билетов:\n\n1. Скопируйте изображение билета (Ctrl+C или Cmd+C)\n2. Вставьте его в чат с ботом (Ctrl+V или Cmd+V)\n3. Бот автоматически распознает билет и добавит его к плану');
      }
    });
  }
});

// Проверка защиты от спама
function checkSpamProtection(url) {
  const now = Date.now();
  const COOLDOWN_MS = 60 * 1000; // 1 минута
  const MAX_REPEATED_REQUESTS = 5; // Максимум 5 одинаковых запросов подряд
  
  // Удаляем старые записи (старше минуты)
  urlRequestHistory = urlRequestHistory.filter(entry => now - entry.timestamp < COOLDOWN_MS);
  
  // Считаем количество одинаковых URL за последнюю минуту
  const recentSameUrls = urlRequestHistory.filter(entry => entry.url === url);
  
  if (recentSameUrls.length >= MAX_REPEATED_REQUESTS) {
    const oldestRequest = recentSameUrls[0];
    const timeLeft = COOLDOWN_MS - (now - oldestRequest.timestamp);
    const secondsLeft = Math.ceil(timeLeft / 1000);
    
    alert(`⏸️ Включился кулдаун на ${secondsLeft} секунд. Пожалуйста, подождите перед следующим запросом.`);
    return false;
  }
  
  // Добавляем текущий запрос в историю
  urlRequestHistory.push({ url, timestamp: now });
  return true;
}

// Функция для загрузки фильма с текущей вкладки (всегда получает свежий URL)
async function loadCurrentTabFilm() {
  if (!chatId) return;
  
  try {
    // ВСЕГДА получаем свежий URL при каждом открытии popup (критично для SPA)
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tabs && tabs[0] && tabs[0].url) {
      const currentUrl = tabs[0].url;
      console.log('[LOAD CURRENT TAB] Получен актуальный URL:', currentUrl, 'Предыдущий:', lastDetectedUrl);
      
      // Обновляем lastDetectedUrl
      const urlChanged = lastDetectedUrl !== currentUrl;
      lastDetectedUrl = currentUrl;
      
      // Загружаем фильм (даже если URL не изменился, т.к. на SPA контент мог измениться)
      await detectAndLoadFilm(currentUrl, urlChanged);
    }
  } catch (error) {
    console.error('Ошибка получения текущей вкладки:', error);
  }
}

// Автоматическое определение и загрузка фильма с текущей страницы
async function detectAndLoadFilm(url, urlChanged = true) {
  if (!url || !chatId) return;
  
  // Проверяем защиту от спама только если URL изменился
  if (urlChanged && !checkSpamProtection(url)) {
    return;
  }
  
  // Для SPA: если URL не изменился, всё равно загружаем,
  // т.к. контент на странице мог измениться без изменения URL
  // Но если это явно тот же URL и мы уже загружали недавно, пропускаем
  if (!urlChanged) {
    // Проверяем, не загружали ли мы этот URL совсем недавно (защита от спама)
    const recentRequest = urlRequestHistory.find(r => r.url === url && Date.now() - r.timestamp < 2000);
    if (recentRequest) {
      console.log('[DETECT] Пропускаем повторную загрузку того же URL (защита от спама)');
      return;
    }
  }
  
  // ОБЯЗАТЕЛЬНО скрываем поиск и форму планирования ПЕРЕД началом загрузки (с !important)
  const searchSection = document.getElementById('search-section');
  if (searchSection) {
    searchSection.classList.add('hidden');
    searchSection.style.display = 'none';
  }
  
  const planningForm = document.getElementById('planning-form');
  if (planningForm) {
    planningForm.classList.add('hidden');
    planningForm.style.display = 'none';
  }
  const streamingMarkLastEl = document.getElementById('streaming-mark-last');
  if (streamingMarkLastEl) {
    streamingMarkLastEl.classList.add('hidden');
    streamingMarkLastEl.style.display = 'none';
  }
  
  const filmInfo = document.getElementById('film-info');
  
  try {
    // Показываем индикатор загрузки
    if (filmInfo) {
      filmInfo.classList.remove('hidden');
      filmInfo.style.display = ''; // Убираем style.display = 'none'
      const titleEl = document.getElementById('film-title');
      if (titleEl) titleEl.textContent = 'Загружаем информацию о фильме';
      const yearEl = document.getElementById('film-year');
      if (yearEl) yearEl.textContent = '';
      const statusEl = document.getElementById('film-status');
      if (statusEl) statusEl.innerHTML = '';
      const actionsEl = document.getElementById('film-actions');
      if (actionsEl) actionsEl.innerHTML = '';
    }
    
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
              await loadFilmByImdbId(response.imdbId, 'imdb');
            } else {
              await loadFilmByImdbId(imdbId, 'imdb');
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
              await loadFilmByImdbId(response.imdbId, 'letterboxd');
            } else {
              // Fallback: получаем название и год
              chrome.tabs.sendMessage(tabs[0].id, { action: 'get_letterboxd_title_year' }, async (fallbackResponse) => {
                if (fallbackResponse && fallbackResponse.title && fallbackResponse.year) {
                  await loadFilmByKeyword(fallbackResponse.title, fallbackResponse.year, 'letterboxd');
                } else {
                  document.getElementById('film-title').textContent = 'Не удалось определить фильм';
                  document.getElementById('film-year').textContent = 'Попробуйте открыть страницу фильма на Кинопоиске или IMDb';
                }
              });
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

    const streamingHosts = ['tvoe.live', 'ivi.ru', 'okko.tv', 'kinopoisk.ru', 'hd.kinopoisk.ru', 'premier.one', 'wink.ru', 'start.ru', 'amediateka.ru', 'rezka.ag', 'rezka.ad', 'hdrezka', 'lordfilm', 'allserial', 'boxserial'];
    let hostname = '';
    try {
      hostname = new URL(url).hostname.toLowerCase();
    } catch (_) {}
    const isStreaming = streamingHosts.some(h => hostname.includes(h));

    if (isStreaming && chatId && userId) {
      console.log('[POPUP] Стриминговый сайт, пробуем получить pageInfo');
      try {
        const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
        console.log('[POPUP] Текущая вкладка:', tabs?.[0]?.id);
        if (tabs && tabs[0]) {
          const pageInfo = await new Promise((resolve) => {
            chrome.tabs.sendMessage(tabs[0].id, { action: 'get_streaming_page_info' }, (r) => {
              if (chrome.runtime.lastError) {
                console.log('[POPUP] Ошибка sendMessage:', chrome.runtime.lastError.message);
                resolve(null);
              } else {
                console.log('[POPUP] pageInfo получен:', r);
                resolve(r || null);
              }
            });
          });
          if (pageInfo && pageInfo.title) {
            // Всегда загружаем данные через API, даже если сезон/серия не определены
            // Это позволит показать информацию о фильме/сериале и предложить действия
            console.log('[POPUP] Вызываем loadFromStreamingPage');
            await loadFromStreamingPage(pageInfo);
            return;
          } else {
            console.log('[POPUP] pageInfo не содержит title, пропускаем');
          }
        }
      } catch (e) {
        console.log('[POPUP] get_streaming_page_info ошибка:', e);
      }
    } else {
      console.log('[POPUP] Не стриминговый сайт или нет chatId/userId:', { isStreaming, chatId, userId });
    }

    if (filmInfo) {
      filmInfo.classList.add('hidden');
      filmInfo.style.display = 'none';
    }
    let showMarkLast = false;
    if (isStreaming && chatId && userId) {
      const data = await chrome.storage.local.get(['movieplanner_last_streaming_overlay']);
      const last = data.movieplanner_last_streaming_overlay;
      if (last && last.hostname === hostname && last.season != null && last.episode != null && last.kp_id) {
        showMarkLast = true;
        const wrap = document.getElementById('streaming-mark-last');
        const label = document.getElementById('streaming-mark-last-label');
        const btn = document.getElementById('streaming-mark-last-btn');
        if (wrap && label && btn) {
          label.textContent = `${last.title || 'Сериал'} — ${last.season} сезон, ${last.episode} серия`;
          wrap.classList.remove('hidden');
          wrap.style.display = '';
          btn.replaceWith(btn.cloneNode(true));
          const newBtn = document.getElementById('streaming-mark-last-btn');
          newBtn.addEventListener('click', async () => {
            try {
              const r = await fetch(`${API_BASE_URL}/api/extension/mark-episode`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: chatId,
                  user_id: userId,
                  kp_id: last.kp_id,
                  film_id: last.film_id,
                  season: last.season,
                  episode: last.episode
                })
              });
              const j = await r.json();
              if (j.success) {
                label.textContent = 'Отмечено ✓';
                newBtn.disabled = true;
              }
            } catch (e) {
              console.error('Ошибка отметки серии:', e);
            }
          });
        }
      }
    }
    if (!showMarkLast) {
      const wrap = document.getElementById('streaming-mark-last');
      if (wrap) { wrap.classList.add('hidden'); wrap.style.display = 'none'; }
    }
    
    setTimeout(() => {
      const searchSection = document.getElementById('search-section');
      if (searchSection) {
        searchSection.classList.remove('hidden');
        searchSection.style.display = '';
      }
    }, 500);
  } catch (error) {
    console.error('Ошибка определения фильма:', error);
    if (filmInfo) {
      filmInfo.classList.add('hidden');
      filmInfo.style.display = 'none';
    }
    
    // Показываем поиск ТОЛЬКО после того, как стало понятно, что фильм не опознался
    setTimeout(() => {
      const searchSection = document.getElementById('search-section');
      if (searchSection) {
        searchSection.classList.remove('hidden');
        searchSection.style.display = '';
      }
    }, 500);
  }
}

function showAuthScreen() {
  document.getElementById('auth-screen').classList.remove('hidden');
  document.getElementById('main-screen').classList.add('hidden');
}

function showMainScreen() {
  console.log('[POPUP] showMainScreen вызван');
  const authScreen = document.getElementById('auth-screen');
  const mainScreen = document.getElementById('main-screen');
  if (authScreen) authScreen.classList.add('hidden');
  if (mainScreen) {
    mainScreen.classList.remove('hidden');
    mainScreen.style.display = 'block';
    console.log('[POPUP] main-screen показан, hidden=', mainScreen.classList.contains('hidden'));
  }
}

async function handleBind() {
  const codeInput = document.getElementById('code-input');
  const statusEl = document.getElementById('status');
  
  if (!codeInput || !statusEl) {
    console.error('Элементы формы авторизации не найдены');
    return;
  }
  
  const code = codeInput.value.trim().toUpperCase();
  
  if (!code) {
    statusEl.textContent = 'Введите код';
    statusEl.className = 'status error';
    return;
  }
  
  statusEl.textContent = 'Проверяем...';
  statusEl.className = 'status';
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/verify?code=${code}`);
    
    if (!response.ok) {
      // Обрабатываем ошибки HTTP
      let errorMessage = 'Ошибка сети';
      try {
        const errorJson = await response.json();
        if (errorJson.error) {
          errorMessage = errorJson.error;
        }
      } catch (e) {
        // Если не удалось распарсить JSON, используем дефолтное сообщение
      }
      statusEl.textContent = errorMessage;
      statusEl.className = 'status error';
      return;
    }
    
    const json = await response.json();
    
    if (json.success && json.chat_id) {
      chatId = json.chat_id;
      userId = json.user_id;
      
      // Проверяем подписку один раз при подключении
      const ticketsAccess = await checkTicketsSubscription();
      
      await chrome.storage.local.set({ 
        linked_chat_id: json.chat_id,
        linked_user_id: json.user_id,
        has_tickets_access: ticketsAccess
      });
      hasTicketsAccess = ticketsAccess;
      
      statusEl.textContent = '✅ Привязано!';
      statusEl.className = 'status success';
      setTimeout(async () => {
        showMainScreen();
        // После привязки автоматически загружаем фильм с текущей страницы
        try {
          await loadCurrentTabFilm();
        } catch (error) {
          console.error('Ошибка загрузки фильма после привязки:', error);
        }
      }, 1000);
    } else {
      // Обрабатываем ошибки от сервера
      let errorMessage = 'Неверный код';
      if (json.error) {
        if (json.error.includes('expired') || json.error.includes('истёк') || json.error.includes('истек')) {
          errorMessage = 'Код истёк';
        } else if (json.error.includes('invalid') || json.error.includes('неверный')) {
          errorMessage = 'Неверный код';
        } else {
          errorMessage = json.error;
        }
      }
      statusEl.textContent = errorMessage;
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

// Переменная для хранения fallback данных
let fallbackFilmData = null;

async function loadFilmByImdbId(imdbId, source = 'imdb') {
  if (!imdbId || !chatId) return;
  
  try {
    // Скрываем результаты поиска
    const searchResults = document.getElementById('search-results');
    if (searchResults) searchResults.classList.add('hidden');
    
    // Показываем индикатор загрузки
    const filmInfo = document.getElementById('film-info');
    if (filmInfo) {
      filmInfo.classList.remove('hidden');
      filmInfo.style.display = ''; // Убираем style.display = 'none'
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      const statusEl = document.getElementById('film-status');
      const actionsEl = document.getElementById('film-actions');
      const confirmationEl = document.getElementById('film-confirmation');
      if (titleEl) titleEl.textContent = 'Загружаем информацию о фильме';
      if (yearEl) yearEl.textContent = '';
      if (statusEl) statusEl.innerHTML = '';
      if (actionsEl) actionsEl.innerHTML = '';
      if (confirmationEl) {
        confirmationEl.classList.add('hidden');
        confirmationEl.style.display = 'none';
      }
    }
    
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?imdb_id=${imdbId}&chat_id=${chatId}`);
    
    let json;
    if (response.ok) {
      json = await response.json();
      
      if (json.success && json.film && json.film.kp_id) {
        displayFilmInfo(json.film, json);
        return; // Успешно загрузили, выходим
      }
    }
    
    // Если не удалось загрузить по imdb_id (404 или пустой результат), используем fallback
    if (source === 'imdb' || source === 'letterboxd') {
      await tryFallbackSearch(imdbId, source);
    } else {
      const filmInfoEl = document.getElementById('film-info');
      if (filmInfoEl) {
        filmInfoEl.classList.remove('hidden');
        filmInfoEl.style.display = '';
      }
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Фильм не найден';
      if (yearEl) {
        let errorText = 'Попробуйте другую ссылку';
        if (response.ok && json) {
          errorText = json.error || errorText;
        } else if (!response.ok) {
          try {
            const errorJson = await response.json();
            errorText = errorJson.error || errorText;
          } catch (e) {
            errorText = 'Ошибка загрузки';
          }
        }
        yearEl.textContent = errorText;
      }
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    const filmInfo = document.getElementById('film-info');
    if (filmInfo) {
      filmInfo.classList.remove('hidden');
      filmInfo.style.display = '';
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Ошибка загрузки';
      if (yearEl) yearEl.textContent = 'Проверьте подключение к интернету';
    }
  }
}

async function tryFallbackSearch(imdbId, source) {
  try {
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tabs || !tabs[0]) return;
    
    let title, year;
    
    if (source === 'imdb') {
      const response = await chrome.tabs.sendMessage(tabs[0].id, { action: 'get_imdb_title_year' });
      if (response && response.title && response.year) {
        title = response.title;
        year = response.year;
      }
    } else if (source === 'letterboxd') {
      const response = await chrome.tabs.sendMessage(tabs[0].id, { action: 'get_letterboxd_title_year' });
      if (response && response.title && response.year) {
        title = response.title;
        year = response.year;
      }
    }
    
    if (title && year) {
      await loadFilmByKeyword(title, year, source);
    } else {
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Фильм не найден';
      if (yearEl) yearEl.textContent = 'Не удалось получить данные со страницы';
    }
  } catch (err) {
    console.error('Ошибка fallback поиска:', err);
  }
}

async function loadFilmByKeyword(keyword, year, source) {
  try {
    const titleEl = document.getElementById('film-title');
    const yearEl = document.getElementById('film-year');
    if (titleEl) titleEl.textContent = 'Ищем фильм по названию...';
    if (yearEl) yearEl.textContent = '';
    
    const response = await fetch(`${API_BASE_URL}/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(keyword)}&year=${year}`);
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const json = await response.json();
    
    if (json.success && json.kp_id) {
      // Загружаем информацию о фильме по найденному kp_id
      const filmResponse = await fetch(`${API_BASE_URL}/api/extension/film-info?kp_id=${json.kp_id}&chat_id=${chatId}`);
      if (filmResponse.ok) {
        const filmJson = await filmResponse.json();
        if (filmJson.success) {
          // Сохраняем данные для подтверждения
          fallbackFilmData = {
            film: filmJson.film,
            data: filmJson,
            source: source
          };
          displayFilmInfo(filmJson.film, filmJson, true); // true = показать подтверждение
        }
      }
    } else {
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Фильм не найден';
      if (yearEl) yearEl.textContent = 'Попробуйте другую ссылку';
    }
  } catch (err) {
    console.error('Ошибка поиска по keyword:', err);
    const titleEl = document.getElementById('film-title');
    const yearEl = document.getElementById('film-year');
    if (titleEl) titleEl.textContent = 'Ошибка поиска';
    if (yearEl) yearEl.textContent = 'Проверьте подключение к интернету';
  }
}

async function loadFromStreamingPage(info) {
  console.log('[POPUP] loadFromStreamingPage вызван с info:', info);
  const filmInfo = document.getElementById('film-info');
  const titleEl = document.getElementById('film-title');
  const yearEl = document.getElementById('film-year');
  if (filmInfo) {
    filmInfo.classList.remove('hidden');
    filmInfo.removeAttribute('style');
    filmInfo.style.cssText = 'display: block !important; visibility: visible !important; opacity: 1 !important;';
    filmInfo.offsetHeight; // Форсируем перерисовку
  }
  if (titleEl) titleEl.textContent = 'Ищем на странице...';
  if (yearEl) yearEl.textContent = '';

  // Очищаем название от года в скобках, части, сезона и т.д.
  let baseTitle = (info.title || '')
    .replace(/\s*\(\d{4}\)\s*$/i, '')           // "(2026)" в конце
    .replace(/\s*\(\d{4}\)$/i, '')               // "(2026)" в конце без пробелов
    .replace(/\s*[—\-].*$/, '')                  // " — ..." после тире
    .replace(/\s*\([^)]*[Чч]асть\s*\d+[^)]*\)\s*$/i, '')  // "(Часть 1)"
    .replace(/\s*\([^)]*[Сс]езон[^)]*\)\s*$/i, '')       // "(Сезон 1)"
    .replace(/\s+серия\s+\d+$/i, '')             // "серия 5"
    .trim();
  baseTitle = baseTitle || info.title;
  const keyword = baseTitle;
  const year = info.year || '';
  const type = info.isSeries ? 'TV_SERIES' : 'FILM';
  const searchUrl = `${API_BASE_URL}/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(keyword)}&type=${type}${year ? `&year=${encodeURIComponent(year)}` : ''}`;
  console.log('[POPUP] Поиск:', { keyword, year, type, searchUrl });

  try {
    let searchRes;
    try {
      searchRes = await streamingApiRequest('GET', searchUrl);
    } catch (e) {
      if (titleEl) titleEl.textContent = 'Ошибка загрузки';
      if (yearEl) yearEl.textContent = (e && e.message) || 'Проверьте подключение';
      return;
    }
    const searchJson = searchRes.data || {};
    console.log('[POPUP] Результат поиска:', searchJson);
    if (!searchJson.success || !searchJson.kp_id) {
      console.log('[POPUP] Фильм не найден в поиске');
      if (titleEl) titleEl.textContent = 'Не найден';
      if (yearEl) yearEl.textContent = 'Попробуйте другую страницу';
      return;
    }

    let filmUrl = `${API_BASE_URL}/api/extension/film-info?kp_id=${searchJson.kp_id}&chat_id=${chatId}&user_id=${userId}`;
    if (info.season != null && info.episode != null) filmUrl += `&season=${info.season}&episode=${info.episode}`;
    console.log('[POPUP] Запрос film-info:', filmUrl);
    let filmRes;
    try {
      filmRes = await streamingApiRequest('GET', filmUrl);
    } catch (e) {
      console.error('[POPUP] Ошибка film-info:', e);
      if (titleEl) titleEl.textContent = 'Ошибка загрузки';
      if (yearEl) yearEl.textContent = (e && e.message) || 'Проверьте подключение';
      return;
    }
    const filmJson = filmRes.data || {};
    console.log('[POPUP] Результат film-info:', filmJson);
    if (!filmJson.success || !filmJson.film) {
      console.log('[POPUP] film-info не успешен');
      if (titleEl) titleEl.textContent = 'Ошибка загрузки';
      if (yearEl) yearEl.textContent = filmJson.error || filmRes.error || '';
      return;
    }

    try {
      console.log('[POPUP] Вызываем displayFilmInfo');
      displayFilmInfo(filmJson.film, filmJson);
      console.log('[POPUP] displayFilmInfo завершён успешно');
    } catch (e) {
      console.error('[POPUP] Ошибка displayFilmInfo:', e);
      if (titleEl) titleEl.textContent = 'Ошибка отображения';
      if (yearEl) yearEl.textContent = e.message || '';
      return;
    }

    // Добавляем streaming-специфичные кнопки
    console.log('[POPUP] Добавляем streaming кнопки, info:', info);
    const actionsEl = document.getElementById('film-actions');
    if (actionsEl) {
      const kpId = searchJson.kp_id;
      const filmId = filmJson.film_id;
      
      if (info.isSeries && info.season != null && info.episode != null) {
        console.log('[POPUP] Сериал с сезоном/серией:', info.season, info.episode);
        // Сериал с определенными сезоном/серией
        const markBtn = document.createElement('button');
        markBtn.textContent = `Отметить серию ${info.season}×${info.episode}`;
        markBtn.className = 'btn btn-primary';
        markBtn.style.marginTop = '8px';
        markBtn.addEventListener('click', async () => {
          markBtn.disabled = true;
          try {
            const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/mark-episode`, {
              chat_id: chatId,
              user_id: userId,
              kp_id: kpId,
              film_id: filmId,
              season: info.season,
              episode: info.episode,
              online_link: info.url || undefined
            });
            if (r.data && r.data.success) markBtn.textContent = 'Отмечено ✓';
            else markBtn.disabled = false;
          } catch (e) {
            console.error('Ошибка отметки серии:', e);
            markBtn.disabled = false;
          }
        });
        actionsEl.appendChild(markBtn);
        
        // Кнопка "Отметить все предыдущие" - только если есть непросмотренные серии ДО текущей
        if ((info.season > 1 || info.episode > 1) && filmJson.has_unwatched_before) {
          const markAllBtn = document.createElement('button');
          markAllBtn.textContent = 'Отметить все предыдущие';
          markAllBtn.className = 'btn btn-secondary';
          markAllBtn.style.marginTop = '4px';
          markAllBtn.addEventListener('click', async () => {
            markAllBtn.disabled = true;
            try {
              const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/mark-episode`, {
                chat_id: chatId,
                user_id: userId,
                kp_id: kpId,
                film_id: filmId,
                season: info.season,
                episode: info.episode,
                mark_all_previous: true,
                online_link: info.url || undefined
              });
              if (r.data && r.data.success) markAllBtn.textContent = 'Отмечено ✓';
              else markAllBtn.disabled = false;
            } catch (e) {
              console.error('Ошибка отметки серий:', e);
              markAllBtn.disabled = false;
            }
          });
          actionsEl.appendChild(markAllBtn);
        }
      } else if (info.isSeries) {
        // Сериал без определенных сезона/серии - показываем ручной ввод
        const helpText = document.createElement('div');
        helpText.style.cssText = 'font-size: 12px; color: #666; margin: 8px 0;';
        helpText.textContent = 'Сезон/серия не определены. Выберите в плеере или укажите вручную:';
        actionsEl.appendChild(helpText);
        
        // Форма ручного ввода
        const manualForm = document.createElement('div');
        manualForm.style.cssText = 'display: flex; gap: 6px; margin: 8px 0; align-items: center;';
        manualForm.innerHTML = `
          <input type="number" id="popup-manual-season" placeholder="Сезон" min="1" style="flex: 1; padding: 6px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px;">
          <input type="number" id="popup-manual-episode" placeholder="Серия" min="1" style="flex: 1; padding: 6px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px;">
        `;
        actionsEl.appendChild(manualForm);
        
        // Кнопка отметки
        const markManualBtn = document.createElement('button');
        markManualBtn.textContent = 'Отметить серию';
        markManualBtn.className = 'btn btn-primary';
        markManualBtn.style.marginTop = '4px';
        markManualBtn.addEventListener('click', async () => {
          const seasonInput = document.getElementById('popup-manual-season');
          const episodeInput = document.getElementById('popup-manual-episode');
          const s = parseInt(seasonInput?.value);
          const e = parseInt(episodeInput?.value);
          if (!s || !e || s < 1 || e < 1) {
            alert('Укажите корректные сезон и серию');
            return;
          }
          markManualBtn.disabled = true;
          try {
            const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/mark-episode`, {
              chat_id: chatId,
              user_id: userId,
              kp_id: kpId,
              film_id: filmId,
              season: s,
              episode: e,
              online_link: info.url || undefined
            });
            if (r.data && r.data.success) markManualBtn.textContent = 'Отмечено ✓';
            else markManualBtn.disabled = false;
          } catch (err) {
            console.error('Ошибка отметки серии:', err);
            markManualBtn.disabled = false;
          }
        });
        actionsEl.appendChild(markManualBtn);
        
        // Кнопка "Отметить все до указанной"
        const markAllManualBtn = document.createElement('button');
        markAllManualBtn.textContent = 'Отметить все до указанной';
        markAllManualBtn.className = 'btn btn-secondary';
        markAllManualBtn.style.marginTop = '4px';
        markAllManualBtn.addEventListener('click', async () => {
          const seasonInput = document.getElementById('popup-manual-season');
          const episodeInput = document.getElementById('popup-manual-episode');
          const s = parseInt(seasonInput?.value);
          const e = parseInt(episodeInput?.value);
          if (!s || !e || s < 1 || e < 1) {
            alert('Укажите корректные сезон и серию');
            return;
          }
          markAllManualBtn.disabled = true;
          try {
            const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/mark-episode`, {
              chat_id: chatId,
              user_id: userId,
              kp_id: kpId,
              film_id: filmId,
              season: s,
              episode: e,
              mark_all_previous: true,
              online_link: info.url || undefined
            });
            if (r.data && r.data.success) markAllManualBtn.textContent = 'Отмечено ✓';
            else markAllManualBtn.disabled = false;
          } catch (err) {
            console.error('Ошибка отметки серий:', err);
            markAllManualBtn.disabled = false;
          }
        });
        actionsEl.appendChild(markAllManualBtn);
        
        // Устанавливаем значения по умолчанию (следующая непросмотренная)
        setTimeout(() => {
          const seasonInput = document.getElementById('popup-manual-season');
          const episodeInput = document.getElementById('popup-manual-episode');
          if (filmJson.next_unwatched_season && filmJson.next_unwatched_episode) {
            if (seasonInput) seasonInput.value = filmJson.next_unwatched_season;
            if (episodeInput) episodeInput.value = filmJson.next_unwatched_episode;
          } else {
            if (seasonInput) seasonInput.value = '1';
            if (episodeInput) episodeInput.value = '1';
          }
        }, 0);
      } else if (!info.isSeries) {
        // Фильм
        // Функция для создания блока оценки
        const createRatingBlock = () => {
          const ratingDiv = document.createElement('div');
          ratingDiv.style.cssText = 'margin-top: 12px; padding: 12px; background: #f8f9fa; border-radius: 8px; text-align: center;';
          ratingDiv.innerHTML = `
            <p style="margin: 0 0 10px 0; font-size: 14px; color: #333;">Оцените фильм:</p>
            <div style="display: flex; justify-content: center; gap: 4px;" id="rating-stars">
              ${[1,2,3,4,5,6,7,8,9,10].map(n => `<button data-rating="${n}" style="width: 28px; height: 28px; padding: 0; border: 1px solid #ddd; border-radius: 4px; background: #fff; cursor: pointer; font-size: 12px; display: flex; align-items: center; justify-content: center;">${n}</button>`).join('')}
            </div>
          `;
          actionsEl.appendChild(ratingDiv);
          
          // Добавляем обработчики
          ratingDiv.querySelectorAll('button[data-rating]').forEach(btn => {
            btn.addEventListener('click', async () => {
              const rating = parseInt(btn.dataset.rating);
              ratingDiv.querySelectorAll('button').forEach(b => b.disabled = true);
              try {
                const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/rate-film`, {
                  chat_id: chatId,
                  user_id: userId,
                  kp_id: kpId,
                  film_id: filmId,
                  rating: rating
                });
                if (r.data && r.data.success) {
                  ratingDiv.innerHTML = `<p style="margin: 0; color: #28a745; font-size: 14px;">✅ Оценка ${rating}/10 сохранена!</p>`;
                } else {
                  ratingDiv.innerHTML = `<p style="margin: 0; color: #dc3545; font-size: 14px;">❌ Ошибка сохранения оценки</p>`;
                }
              } catch (e) {
                console.error('Ошибка отправки оценки:', e);
                ratingDiv.innerHTML = `<p style="margin: 0; color: #dc3545; font-size: 14px;">❌ Ошибка сохранения оценки</p>`;
              }
            });
          });
        };
        
        // Если фильм уже просмотрен
        if (filmJson.watched) {
          // Если ещё не оценён - показываем блок оценки
          if (!filmJson.rated) {
            createRatingBlock();
          } else {
            const watchedLabel = document.createElement('p');
            watchedLabel.style.cssText = 'margin: 8px 0; color: #28a745; font-size: 14px;';
            watchedLabel.textContent = '✅ Фильм уже просмотрен и оценён';
            actionsEl.appendChild(watchedLabel);
          }
        } else {
          // Если не просмотрен - показываем кнопку отметки
          const markBtn = document.createElement('button');
          markBtn.textContent = 'Отметить фильм просмотренным';
          markBtn.className = 'btn btn-primary';
          markBtn.style.marginTop = '8px';
          markBtn.addEventListener('click', async () => {
            markBtn.disabled = true;
            markBtn.textContent = '⏳ Отмечаем...';
            try {
              const r = await streamingApiRequest('POST', `${API_BASE_URL}/api/extension/mark-film-watched`, {
                chat_id: chatId,
                user_id: userId,
                kp_id: kpId,
                film_id: filmId,
                online_link: info.url || undefined
              });
              if (r.data && r.data.success) {
                markBtn.textContent = '✅ Просмотрено!';
                markBtn.style.background = '#28a745';
                // Показываем блок оценки через секунду
                setTimeout(() => {
                  markBtn.remove();
                  createRatingBlock();
                }, 1000);
              } else {
                markBtn.textContent = '❌ Ошибка';
                markBtn.disabled = false;
                setTimeout(() => {
                  markBtn.textContent = 'Отметить фильм просмотренным';
                }, 2000);
              }
            } catch (e) {
              console.error('Ошибка отметки фильма:', e);
              markBtn.textContent = '❌ Ошибка';
              markBtn.disabled = false;
              setTimeout(() => {
                markBtn.textContent = 'Отметить фильм просмотренным';
              }, 2000);
            }
          });
          actionsEl.appendChild(markBtn);
        }
      }
    }
  } catch (err) {
    console.error('Ошибка loadFromStreamingPage:', err);
    if (titleEl) titleEl.textContent = 'Ошибка загрузки';
    if (yearEl) yearEl.textContent = (err && err.message) || 'Попробуйте позже';
  }
  
  // ФИНАЛЬНАЯ ПРОВЕРКА: убеждаемся что элементы видны
  console.log('[POPUP] loadFromStreamingPage ЗАВЕРШЁН, финальная проверка:');
  const finalFilmInfo = document.getElementById('film-info');
  const finalTitle = document.getElementById('film-title');
  console.log('[POPUP] ФИНАЛ: filmInfo.display=', finalFilmInfo?.style?.display, 'title=', finalTitle?.textContent, 'height=', finalFilmInfo?.offsetHeight);
  
  // Скрываем оригинальный film-info (будем использовать streaming-film-info)
  if (finalFilmInfo) {
    finalFilmInfo.style.display = 'none';
  }
  
  // Создаём блок с информацией о фильме напрямую в контейнере
  const mainContainer = document.querySelector('#main-screen .container');
  const header = mainContainer?.querySelector('.header');
  if (mainContainer && header && finalTitle?.textContent) {
    // Удаляем старый блок если есть
    const oldBlock = document.getElementById('streaming-film-info');
    if (oldBlock) oldBlock.remove();
    
    const filmDiv = document.createElement('div');
    filmDiv.id = 'streaming-film-info';
    filmDiv.style.cssText = 'background: white; border: 1px solid #e0e0e0; padding: 20px; margin-top: 15px; border-radius: 12px; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15);';
    filmDiv.innerHTML = `
      <h2 style="margin: 0 0 8px 0; color: #333; font-size: 20px; font-weight: 600;">${finalTitle.textContent}</h2>
      <p style="margin: 0 0 15px 0; color: #666; font-size: 14px;">${document.getElementById('film-year')?.textContent || ''}</p>
      <div id="streaming-actions" style="display: flex; flex-direction: column; gap: 8px;"></div>
    `;
    
    // Вставляем после header
    header.insertAdjacentElement('afterend', filmDiv);
    
    // ПЕРЕМЕЩАЕМ оригинальные кнопки (чтобы сохранить обработчики)
    const actionsEl = document.getElementById('film-actions');
    const streamingActions = document.getElementById('streaming-actions');
    if (actionsEl && streamingActions) {
      while (actionsEl.firstChild) {
        const btn = actionsEl.firstChild;
        if (btn.tagName === 'BUTTON') {
          btn.style.cssText = 'padding: 12px !important; border-radius: 8px !important; border: none !important; cursor: pointer !important; font-size: 14px !important; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important; color: white !important; width: 100% !important; font-weight: 500 !important; box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3) !important;';
        }
        streamingActions.appendChild(btn);
      }
    }
    
    console.log('[POPUP] Блок стриминга создан с кнопками:', streamingActions?.children?.length);
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

function displayFilmInfo(film, data, showConfirmation = false) {
  console.log('[DISPLAY FILM] displayFilmInfo вызвана, film:', film, 'data:', data, 'showConfirmation:', showConfirmation);
  
  // ВСЕГДА сначала скрываем блок подтверждения - он показывается ТОЛЬКО при фолбек-поиске
  const confirmationEl = document.getElementById('film-confirmation');
  if (confirmationEl) {
    confirmationEl.classList.add('hidden');
    confirmationEl.style.display = 'none';
  }
  
  // Если открыт режим auto_plan_cinema, автоматически открываем форму планирования
  if (window.autoPlanCinemaMode) {
    window.autoPlanCinemaMode = false; // Сбрасываем флаг
    // Скрываем поиск
    const searchSection = document.getElementById('search-section');
    if (searchSection) {
      searchSection.classList.add('hidden');
      searchSection.style.display = 'none';
    }
    // Автоматически открываем форму планирования с выбором "В кино"
    setTimeout(() => {
      setPlanType('cinema');
      showPlanningForm();
    }, 300);
  } else {
    // ОБЯЗАТЕЛЬНО скрываем поиск, если фильм опознался
    const searchSection = document.getElementById('search-section');
    if (searchSection) {
      searchSection.classList.add('hidden');
      searchSection.style.display = 'none';
    }
  }
  
  // ОБЯЗАТЕЛЬНО скрываем форму планирования (она показывается ТОЛЬКО при клике на кнопку)
  const planningForm = document.getElementById('planning-form');
  if (planningForm) {
    planningForm.classList.add('hidden');
    planningForm.style.display = 'none';
  }
  
  // Очищаем предыдущее состояние
  currentFilm = null;
  
  // Устанавливаем новое состояние
  currentFilm = film;
  currentFilm.film_id = data.film_id;
  
  console.log('[DISPLAY FILM] currentFilm установлен:', currentFilm, 'kp_id:', currentFilm.kp_id);
  
  // ОБЯЗАТЕЛЬНО показываем блок film-info
  const filmInfo = document.getElementById('film-info');
  console.log('[DISPLAY FILM] filmInfo элемент ДО:', filmInfo?.classList?.toString(), filmInfo?.style?.display);
  if (filmInfo) {
    // Сначала убираем все стили, которые могут скрывать элемент
    filmInfo.classList.remove('hidden');
    filmInfo.removeAttribute('style'); // Убираем все инлайн стили
    
    // Устанавливаем видимость
    filmInfo.style.cssText = 'display: block !important; visibility: visible !important; opacity: 1 !important;';
    
    // Форсируем перерисовку браузера
    filmInfo.offsetHeight; // Reflow trick
    
    console.log('[DISPLAY FILM] film-info ПОСЛЕ:', filmInfo.classList.toString(), filmInfo.style.display, filmInfo.offsetHeight);
  } else {
    console.error('[DISPLAY FILM] ОШИБКА: film-info элемент не найден!');
  }
  
  const titleEl = document.getElementById('film-title');
  const yearEl = document.getElementById('film-year');
  console.log('[DISPLAY FILM] titleEl:', titleEl, 'yearEl:', yearEl);
  if (titleEl) {
    titleEl.textContent = film.title || 'Без названия';
    titleEl.style.color = '#333'; // Убедимся что текст виден
    console.log('[DISPLAY FILM] Название установлено:', film.title, 'innerHTML=', titleEl.innerHTML);
  }
  if (yearEl) {
    yearEl.textContent = film.year || '';
    yearEl.style.color = '#666';
  }
  
  // ОТЛАДКА: Проверяем что элементы видны
  console.log('[DISPLAY FILM] ПРОВЕРКА ВИДИМОСТИ:', {
    filmInfo_display: filmInfo?.style?.display,
    filmInfo_visibility: filmInfo?.style?.visibility,
    filmInfo_offsetHeight: filmInfo?.offsetHeight,
    filmInfo_offsetWidth: filmInfo?.offsetWidth,
    title_text: titleEl?.textContent,
    mainScreen_hidden: document.getElementById('main-screen')?.classList?.contains('hidden')
  });
  
  const statusEl = document.getElementById('film-status');
  console.log('[DISPLAY FILM] statusEl:', statusEl);
  if (statusEl) {
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
  }
  
  const actionsEl = document.getElementById('film-actions');
  console.log('[DISPLAY FILM] actionsEl:', actionsEl);
  if (!actionsEl) {
    console.error('[DISPLAY FILM] ОШИБКА: film-actions элемент не найден!');
    return;
  }
  actionsEl.innerHTML = '';
  
  // ОТЛАДКА: Добавляем тестовый текст, чтобы убедиться что кнопки создаются
  console.log('[DISPLAY FILM] Создаём кнопки, data.in_database=', data.in_database);
  
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
        // После успешного добавления меняем кнопку на "Удалить"
        dbBtn.textContent = '✅ Добавлено!';
        setTimeout(() => {
          dbBtn.textContent = '🗑️ Удалить из базы';
          dbBtn.className = 'btn btn-secondary';
          dbBtn.disabled = false;
          // Меняем обработчик на удаление
          dbBtn.onclick = async () => {
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
          };
        }, 1500);
      } catch (e) {
        dbBtn.textContent = '❌ Ошибка';
        dbBtn.disabled = false;
        setTimeout(() => {
          dbBtn.textContent = '➕ Добавить в базу';
        }, 2000);
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
  
  console.log('[DISPLAY FILM] Кнопки добавлены, actionsEl.innerHTML=', actionsEl.innerHTML.substring(0, 100));
  
  // Если есть план "в кино", добавляем кнопку "Добавить билеты"
  if (data.has_plan && data.plan_type === 'cinema' && data.plan_id && hasTicketsAccess) {
    const ticketsBtn = document.createElement('button');
    ticketsBtn.textContent = '🎟️ Добавить билеты';
    ticketsBtn.className = 'btn btn-secondary';
    ticketsBtn.style.marginTop = '10px';
    ticketsBtn.addEventListener('click', async () => {
      if (isProcessing) return;
      isProcessing = true;
      ticketsBtn.disabled = true;
      ticketsBtn.textContent = '⏳ Отправляем...';
      
      try {
        const response = await fetch(`${API_BASE_URL}/api/extension/init-ticket-upload`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            user_id: userId,
            plan_id: data.plan_id
          })
        });
        
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            alert('✅ Сообщение отправлено в бота. Отправьте фото или файл с билетом(ами) в чат.');
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          const errorJson = await response.json();
          alert('Ошибка: ' + (errorJson.error || 'не удалось отправить сообщение'));
        }
      } catch (err) {
        console.error('Ошибка инициализации загрузки билетов:', err);
        alert('Ошибка. Попробуйте отправить билет напрямую в чат с ботом.');
      } finally {
        isProcessing = false;
        ticketsBtn.disabled = false;
        ticketsBtn.textContent = '🎟️ Добавить билеты';
      }
    });
    actionsEl.appendChild(ticketsBtn);
  }
  
  // Показываем подтверждение ТОЛЬКО если это fallback поиск (showConfirmation === true)
  if (showConfirmation === true && confirmationEl) {
    confirmationEl.classList.remove('hidden');
    confirmationEl.style.display = '';
    
    // Обработчики кнопок подтверждения
    const confirmYesBtn = document.getElementById('confirm-film-yes');
    const confirmNoBtn = document.getElementById('confirm-film-no');
    
    if (confirmYesBtn) {
      confirmYesBtn.onclick = () => {
        // Подтверждаем - скрываем блок подтверждения
        if (confirmationEl) {
          confirmationEl.classList.add('hidden');
          confirmationEl.style.display = 'none';
        }
        fallbackFilmData = null;
      };
    }
    
    if (confirmNoBtn) {
      confirmNoBtn.onclick = () => {
        // Отклоняем - скрываем информацию о фильме
        if (confirmationEl) {
          confirmationEl.classList.add('hidden');
          confirmationEl.style.display = 'none';
        }
        const filmInfoEl = document.getElementById('film-info');
        if (filmInfoEl) {
          filmInfoEl.classList.add('hidden');
          filmInfoEl.style.display = 'none';
        }
        const searchSection = document.getElementById('search-section');
        if (searchSection) {
          searchSection.classList.remove('hidden');
          searchSection.style.display = '';
        }
        fallbackFilmData = null;
      };
    }
  }
  
  // Убеждаемся, что film-info видим (filmInfo уже объявлен выше)
  if (filmInfo) {
    filmInfo.classList.remove('hidden');
    filmInfo.style.display = ''; // Убираем style.display = 'none'
  }
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
      isProcessing = false;
    }
  } catch (err) {
    console.error('[ADD FILM] Ошибка в catch блоке:', err);
    console.error('[ADD FILM] Stack trace:', err.stack);
    const errorMessage = err.message || 'Проверьте подключение к интернету';
    console.error('[ADD FILM] Показываем alert с ошибкой:', errorMessage);
    alert('Ошибка добавления фильма: ' + errorMessage);
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
    // Скрываем результаты поиска
    const searchResults = document.getElementById('search-results');
    if (searchResults) searchResults.classList.add('hidden');
    
    // Показываем индикатор загрузки
    const filmInfo = document.getElementById('film-info');
    if (filmInfo) {
      filmInfo.classList.remove('hidden');
      filmInfo.style.display = ''; // Убираем style.display = 'none'
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      const statusEl = document.getElementById('film-status');
      const actionsEl = document.getElementById('film-actions');
      if (titleEl) titleEl.textContent = 'Загружаем информацию о фильме';
      if (yearEl) yearEl.textContent = '';
      if (statusEl) statusEl.innerHTML = '';
      if (actionsEl) actionsEl.innerHTML = '';
    }
    
    const response = await fetch(`${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${chatId}`);
    
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
      displayFilmInfo(json.film, json);
    } else {
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Фильм не найден';
      if (yearEl) yearEl.textContent = json.error || 'Попробуйте другую ссылку';
    }
  } catch (err) {
    console.error('Ошибка загрузки фильма:', err);
    const filmInfo = document.getElementById('film-info');
    if (filmInfo) {
      filmInfo.classList.remove('hidden');
      const titleEl = document.getElementById('film-title');
      const yearEl = document.getElementById('film-year');
      if (titleEl) titleEl.textContent = 'Ошибка загрузки';
      if (yearEl) yearEl.textContent = 'Проверьте подключение к интернету';
    }
  }
}

async function showPlanningForm() {
  // Если фильм не в базе, сначала автоматически добавляем его
  if (!currentFilm || !currentFilm.film_id) {
    if (currentFilm && currentFilm.kp_id) {
      // Автоматически добавляем в базу, если еще не добавлен
      try {
        await addFilmToDatabase(currentFilm.kp_id);
        // После добавления перезагружаем информацию о фильме
        if (currentFilm.kp_id) {
          await loadFilmByKpId(currentFilm.kp_id);
        } else if (currentFilm.imdb_id) {
          await loadFilmByImdbId(currentFilm.imdb_id);
        }
        // Проверяем, что фильм теперь в базе
        if (currentFilm && currentFilm.film_id) {
          const planningForm = document.getElementById('planning-form');
          if (planningForm) {
            planningForm.classList.remove('hidden');
            planningForm.style.display = '';
            initializePlanningForm();
          }
        }
      } catch (err) {
        console.error('Ошибка при автоматическом добавлении фильма:', err);
        alert('Не удалось добавить фильм в базу. Попробуйте еще раз.');
      }
      return;
    } else {
      alert('Не удалось определить фильм. Попробуйте добавить его вручную.');
      return;
    }
  }
  
  // Показываем форму планирования ТОЛЬКО когда пользователь явно нажал кнопку
  const planningForm = document.getElementById('planning-form');
  if (planningForm) {
    planningForm.classList.remove('hidden');
    planningForm.style.display = '';
    initializePlanningForm();
  }
}

function initializePlanningForm() {
  // Сбрасываем выбор типа плана на "Дома"
  setPlanType('home');
  
  // Устанавливаем минимальную дату (сегодня) и предустанавливаем текущий год
  const now = new Date();
  now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
  document.getElementById('plan-datetime').min = now.toISOString().slice(0, 16);
  
  // Сбрасываем галочку календаря (по умолчанию используем текстовое поле)
  const calendarCheckbox = document.getElementById('use-calendar-checkbox');
  const planDatetime = document.getElementById('plan-datetime');
  const planTimeText = document.getElementById('plan-time-text');
  
  if (calendarCheckbox) {
    calendarCheckbox.checked = false;
  }
  
  // Настраиваем поля в зависимости от состояния галочки
  if (planDatetime && planTimeText) {
    planDatetime.disabled = true;
    planDatetime.style.backgroundColor = '#f0f0f0';
    planDatetime.value = ''; // Очищаем календарь
    planTimeText.disabled = false;
    planTimeText.style.backgroundColor = '';
    planTimeText.value = ''; // Очищаем текстовое поле
  }
  
  // Предустанавливаем текущий год в календаре (если не декабрь), даже если он неактивен
  const currentMonth = now.getMonth() + 1; // 1-12
  if (currentMonth !== 12 && planDatetime) {
    const defaultDate = new Date(now);
    defaultDate.setHours(19, 0, 0, 0); // 19:00 по умолчанию
    planDatetime.min = defaultDate.toISOString().slice(0, 16);
  }
}

let selectedPlanType = 'home'; // По умолчанию "Дома"
let hasTicketsAccess = false; // Кэшируем статус подписки

// Проверка подписки один раз при подключении
async function checkTicketsSubscription() {
  if (!chatId || !userId) return false;
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/check-subscription?chat_id=${chatId}&user_id=${userId}`);
    if (response.ok) {
      const json = await response.json();
      if (json.success) {
        hasTicketsAccess = json.has_tickets_access || false;
        return hasTicketsAccess;
      }
    }
  } catch (err) {
    console.error('Ошибка проверки подписки:', err);
  }
  return false;
}

function setPlanType(type) {
  selectedPlanType = type;
  
  // Обновляем классы кнопок
  const homeBtn = document.getElementById('plan-type-home');
  const cinemaBtn = document.getElementById('plan-type-cinema');
  const streamingEl = document.getElementById('streaming-services');
  const addTicketsBtn = document.getElementById('add-tickets-btn');
  
  if (type === 'home') {
    if (homeBtn) {
      homeBtn.classList.remove('btn-secondary');
      homeBtn.classList.add('btn-primary', 'active');
      homeBtn.style.border = '2px solid #007bff';
    }
    if (cinemaBtn) {
      cinemaBtn.classList.remove('btn-primary', 'active');
      cinemaBtn.classList.add('btn-secondary');
      cinemaBtn.style.border = '2px solid transparent';
    }
    if (streamingEl) streamingEl.classList.remove('hidden');
    // Скрываем кнопку билетов при выборе "Дома"
    if (addTicketsBtn) {
      addTicketsBtn.classList.add('hidden');
    }
    // Загружаем список онлайн-кинотеатров из API
    if (currentFilm && currentFilm.kp_id) {
      loadStreamingServices(currentFilm.kp_id);
    }
  } else {
    if (cinemaBtn) {
      cinemaBtn.classList.remove('btn-secondary');
      cinemaBtn.classList.add('btn-primary', 'active');
      cinemaBtn.style.border = '2px solid #007bff';
    }
    if (homeBtn) {
      homeBtn.classList.remove('btn-primary', 'active');
      homeBtn.classList.add('btn-secondary');
      homeBtn.style.border = '2px solid transparent';
    }
    if (streamingEl) streamingEl.classList.add('hidden');
    // Показываем кнопку билетов при выборе "В кино"
    if (addTicketsBtn) {
      addTicketsBtn.classList.remove('hidden');
      addTicketsBtn.disabled = !hasTicketsAccess;
      if (!hasTicketsAccess) {
        addTicketsBtn.title = 'Оформите подписку "Билеты" для загрузки билетов на мероприятия';
      } else {
        addTicketsBtn.title = '';
      }
    }
  }
}

async function loadStreamingServices(kpId) {
  if (!kpId) return;
  
  const streamingEl = document.getElementById('streaming-services');
  const select = document.getElementById('streaming-service');
  
  if (!streamingEl || !select) return;
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/streaming-services?kp_id=${kpId}`);
    if (!response.ok) {
      console.error('Ошибка загрузки стриминговых сервисов:', response.status);
      // Скрываем поле, если не удалось загрузить
      streamingEl.classList.add('hidden');
      return;
    }
    
    const json = await response.json();
    
    // Очищаем текущие опции (кроме первой "Выберите сервис")
    select.innerHTML = '<option value="">Выберите сервис</option>';
    
    if (json.success && json.services && json.services.length > 0) {
      json.services.forEach(service => {
        const option = document.createElement('option');
        option.value = service.name;
        option.textContent = service.name;
        option.setAttribute('data-url', service.url || ''); // Сохраняем URL в data-атрибуте
        select.appendChild(option);
      });
      // Показываем поле только если есть сервисы
      streamingEl.classList.remove('hidden');
    } else {
      // Скрываем поле, если нет сервисов
      streamingEl.classList.add('hidden');
    }
  } catch (err) {
    console.error('Ошибка загрузки стриминговых сервисов:', err);
    // Скрываем поле при ошибке
    streamingEl.classList.add('hidden');
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
  if (!createBtn) {
    isProcessing = false;
    return;
  }
  const originalText = createBtn.textContent;
  createBtn.disabled = true;
  createBtn.textContent = '⏳ Создаём план...';
  
  try {
    const planType = selectedPlanType;
    const planTimeTextEl = document.getElementById('plan-time-text');
    const planDatetimeEl = document.getElementById('plan-datetime');
    const streamingServiceEl = document.getElementById('streaming-service');
    
    const planTimeText = planTimeTextEl ? planTimeTextEl.value.trim() : '';
    const planDatetime = planDatetimeEl ? planDatetimeEl.value : '';
    const streamingService = streamingServiceEl ? streamingServiceEl.value : '';
    // Получаем URL из выбранной опции
    const streamingUrl = streamingServiceEl && streamingServiceEl.selectedOptions[0] 
      ? streamingServiceEl.selectedOptions[0].getAttribute('data-url') || null 
      : null;
  
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
  
  // Создаем план
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
        streaming_url: streamingUrl || null
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
      // Если план "в кино", автоматически активируем ожидание билета
      if (selectedPlanType === 'cinema' && json.plan_id && hasTicketsAccess) {
        try {
          const ticketResponse = await fetch(`${API_BASE_URL}/api/extension/init-ticket-upload`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              user_id: userId,
              plan_id: json.plan_id
            })
          });
          
          if (ticketResponse.ok) {
            const ticketResult = await ticketResponse.json();
            if (ticketResult.success) {
              alert('✅ План создан!\n\n🎟️ Для сохранения билетов отправьте скриншот в чат с ботом — он уже ждёт ваш билет.\n\n💡 Напоминание о событии вместе с билетами придёт незадолго до сеанса!');
            } else {
              alert('✅ План создан!\n\n🎟️ Для добавления билетов отправьте скриншот в чат с ботом.');
            }
          } else {
            alert('✅ План создан!\n\n🎟️ Для добавления билетов отправьте скриншот в чат с ботом.');
          }
        } catch (err) {
          console.error('Ошибка инициализации загрузки билетов:', err);
          alert('✅ План создан!\n\n🎟️ Для добавления билетов отправьте скриншот в чат с ботом.');
        }
      } else {
        alert('✅ План создан!');
      }
      
      // Показываем кнопку "Добавить билеты" после успешного планирования, если выбрано "В кино"
      const addTicketsBtn = document.getElementById('add-tickets-btn');
      if (selectedPlanType === 'cinema' && addTicketsBtn && hasTicketsAccess && json.plan_id) {
        addTicketsBtn.classList.remove('hidden');
        addTicketsBtn.disabled = false;
        addTicketsBtn.title = '';
        // Инициируем сообщение в боте для загрузки билетов
        addTicketsBtn.onclick = async () => {
          try {
            // Отправляем запрос в бот для начала процесса загрузки билетов
            const response = await fetch(`${API_BASE_URL}/api/extension/init-ticket-upload`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                user_id: userId,
                plan_id: json.plan_id
              })
            });
            
            if (response.ok) {
              const result = await response.json();
              if (result.success) {
                alert('✅ Сообщение отправлено в бота. Отправьте фото или файл с билетом(ами) в чат.');
              } else {
                alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
              }
            } else {
              const errorJson = await response.json();
              alert('Ошибка: ' + (errorJson.error || 'не удалось отправить сообщение'));
            }
          } catch (err) {
            console.error('Ошибка инициализации загрузки билетов:', err);
            alert('Ошибка. Попробуйте отправить билет напрямую в чат с ботом.');
          }
        };
      }
      
      // Закрываем форму планирования через 3 секунды, чтобы пользователь мог нажать кнопку билетов
      // Но только если не в режиме auto_plan_cinema (там уже показали сообщение)
      if (!window.autoPlanCinemaMode) {
        setTimeout(() => {
          const planningForm = document.getElementById('planning-form');
          if (planningForm) {
            planningForm.classList.add('hidden');
            planningForm.style.display = 'none';
          }
          // Завершаем процесс работы с фильмом - очищаем состояние
          resetExtensionState();
          // Очищаем информацию о фильме
          const filmInfo = document.getElementById('film-info');
          if (filmInfo) {
            filmInfo.classList.add('hidden');
            filmInfo.style.display = 'none';
          }
        }, 3000);
      } else {
        // В режиме auto_plan_cinema сразу закрываем форму после показа сообщения
        setTimeout(() => {
          const planningForm = document.getElementById('planning-form');
          if (planningForm) {
            planningForm.classList.add('hidden');
            planningForm.style.display = 'none';
          }
          resetExtensionState();
          const filmInfo = document.getElementById('film-info');
          if (filmInfo) {
            filmInfo.classList.add('hidden');
            filmInfo.style.display = 'none';
          }
        }, 1000);
      }
    } else {
      alert('Ошибка создания плана: ' + (json.error || 'неизвестная ошибка'));
    }
  } catch (err) {
    console.error('Ошибка создания плана:', err);
    alert('Ошибка создания плана: ' + (err.message || 'Проверьте подключение к интернету'));
  } finally {
    isProcessing = false;
    if (createBtn) {
      createBtn.disabled = false;
      createBtn.textContent = originalText;
    }
  }
}

async function performSearch() {
  const query = document.getElementById('search-input').value.trim();
  if (!query) {
    alert('Введите название фильма или сериала');
    return;
  }
  
  const resultsEl = document.getElementById('search-results');
  const searchBtn = document.getElementById('search-btn');
  
  if (resultsEl) {
    resultsEl.classList.remove('hidden');
    resultsEl.innerHTML = '<p>🔍 Ищем...</p>';
  }
  
  if (searchBtn) {
    searchBtn.disabled = true;
    searchBtn.textContent = '⏳ Поиск...';
  }
  
  try {
    const response = await fetch(`${API_BASE_URL}/api/extension/search?query=${encodeURIComponent(query)}&page=1`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const json = await response.json();
    
    if (searchBtn) {
      searchBtn.disabled = false;
      searchBtn.textContent = '🔍 Найти';
    }
    
    if (json.success && json.results && json.results.length > 0) {
      // Скрываем информацию о фильме и показываем результаты поиска
      const filmInfo = document.getElementById('film-info');
      if (filmInfo) filmInfo.classList.add('hidden');
      
      if (resultsEl) {
        let html = '<div class="search-results-list">';
        json.results.forEach((film, idx) => {
          const typeEmoji = film.is_series ? '📺' : '🎬';
          const yearText = film.year ? ` (${film.year})` : '';
          html += `
            <div class="search-result-item" data-kp-id="${film.kp_id}">
              <div class="search-result-title">${typeEmoji} ${film.title}${yearText}</div>
            </div>
          `;
        });
        html += '</div>';
        
        if (json.total_pages > 1) {
          html += `<p class="search-more">Показано ${json.results.length} результатов. Используйте /search в боте для полного поиска.</p>`;
        }
        
        resultsEl.innerHTML = html;
        
        // Добавляем обработчики кликов на результаты
        resultsEl.querySelectorAll('.search-result-item').forEach(item => {
          item.addEventListener('click', async () => {
            const kpId = item.getAttribute('data-kp-id');
            if (kpId) {
              // Скрываем результаты поиска
              resultsEl.classList.add('hidden');
              // Загружаем фильм
              await loadFilmByKpId(kpId);
            }
          });
        });
      }
    } else {
      if (resultsEl) {
        resultsEl.innerHTML = '<p>😔 Фильмы не найдены. Попробуйте другой запрос или используйте /search в боте.</p>';
      }
    }
  } catch (err) {
    console.error('Ошибка поиска:', err);
    if (resultsEl) {
      resultsEl.innerHTML = '<p class="error">Ошибка поиска. Проверьте подключение к интернету.</p>';
    }
    if (searchBtn) {
      searchBtn.disabled = false;
      searchBtn.textContent = '🔍 Найти';
    }
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
  
  const backBtn = document.getElementById('back-btn');
  if (backBtn) {
    backBtn.addEventListener('click', () => {
      window.location.href = 'popup.html';
    });
  }
}
