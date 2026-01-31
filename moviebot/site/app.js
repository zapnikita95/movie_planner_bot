/**
 * Movie Planner — личный кабинет на сайте
 * API: https://web-production-3921c.up.railway.app
 */
(function () {
  'use strict';

  const API_BASE = 'https://web-production-3921c.up.railway.app';
  const BOT_LINK = 'https://t.me/movie_planner_bot';
  const BOT_START_LINK = 'https://t.me/movie_planner_bot?start=start';
  const BOT_CODE_LINK = 'https://t.me/movie_planner_bot?start=code';
  const BOT_SEARCH_LINK = BOT_LINK + '?start=search';
  const BOT_PREMIERES_LINK = BOT_LINK + '?start=premieres';
  const BOT_RANDOM_LINK = BOT_LINK + '?start=random';
  let cabinetHasData = false;

  function posterUrl(kpId) {
    if (!kpId) return '';
    return 'https://st.kp.yandex.net/images/film_big/' + String(kpId).replace(/\D/g, '') + '.jpg';
  }

  const STORAGE_SESSIONS = 'mp_site_sessions';
  const STORAGE_ACTIVE = 'mp_site_active_chat_id';
  const MAX_PERSONAL = 2;
  const MAX_GROUP = 2;

  function getSessions() {
    try {
      const raw = localStorage.getItem(STORAGE_SESSIONS);
      return raw ? JSON.parse(raw) : [];
    } catch (_) {
      return [];
    }
  }

  function setSessions(sessions) {
    localStorage.setItem(STORAGE_SESSIONS, JSON.stringify(sessions));
  }

  function getActiveChatId() {
    return localStorage.getItem(STORAGE_ACTIVE);
  }

  function setActiveChatId(chatId) {
    if (chatId != null && chatId !== '') localStorage.setItem(STORAGE_ACTIVE, String(chatId));
    else localStorage.removeItem(STORAGE_ACTIVE);
  }

  function getToken() {
    const sessions = getSessions();
    const active = getActiveChatId();
    const session = sessions.find((s) => String(s.chat_id) === String(active));
    return session ? session.token : null;
  }

  function removeSessionByToken(token) {
    const sessions = getSessions();
    const removed = sessions.find((s) => s.token === token);
    const active = getActiveChatId();
    const wasActive = removed && String(removed.chat_id) === String(active);
    const next = sessions.filter((s) => s.token !== token);
    setSessions(next);
    if (wasActive) {
      if (next.length) setActiveChatId(next[0].chat_id);
      else setActiveChatId(null);
    }
  }

  function api(url, options = {}) {
    const token = getToken();
    const headers = { 'Content-Type': 'application/json', ...options.headers };
    if (token) headers['Authorization'] = 'Bearer ' + token;
    return fetch(API_BASE + url, { ...options, headers }).then((r) => {
      if (r.status === 401 && token) {
        removeSessionByToken(token);
        if (!getActiveChatId()) window.dispatchEvent(new CustomEvent('mp:logout'));
      }
      return r.json().catch(() => ({}));
    });
  }

  function filmDeepLink(kpId, isSeries) {
    const type = isSeries ? 'series' : 'film';
    return `${BOT_LINK}?start=view_${type}_${kpId}`;
  }

  function planDeepLink() {
    return BOT_START_LINK;
  }

  // ——— UI: шапка, выпадающее меню аккаунтов ———
  function closeAccountDropdown() {
    const dd = document.getElementById('header-account-dropdown');
    if (dd) dd.classList.add('hidden');
  }

  function openAccountDropdown() {
    const dd = document.getElementById('header-account-dropdown');
    if (!dd) return;
    const sessions = getSessions();
    const activeId = getActiveChatId();
    const personalCount = sessions.filter((s) => s.is_personal).length;
    const groupCount = sessions.filter((s) => !s.is_personal).length;
    const canAddPersonal = personalCount < MAX_PERSONAL;
    const canAddGroup = groupCount < MAX_GROUP;
    const canAdd = sessions.length < MAX_PERSONAL + MAX_GROUP && (canAddPersonal || canAddGroup);

    let html = '';
    if (sessions.length) {
      html += '<div class="header-dropdown-title">Текущие входы</div>';
      sessions.forEach((s) => {
        const isActive = String(s.chat_id) === String(activeId);
        const typeLabel = s.is_personal ? 'личный' : 'группа';
        const name = escapeHtml(s.name || 'Кабинет');
        html += '<div class="header-dropdown-account' + (isActive ? ' is-active' : '') + '" data-chat-id="' + escapeHtml(String(s.chat_id)) + '">';
        html += '<span class="header-dropdown-account-name">' + name + '<span class="header-dropdown-account-type">(' + typeLabel + ')</span></span>';
        html += '<button type="button" class="header-dropdown-account-remove" data-chat-id="' + escapeHtml(String(s.chat_id)) + '" aria-label="Выйти">×</button>';
        html += '</div>';
      });
      html += '<div class="header-dropdown-divider"></div>';
    }
    html += '<button type="button" class="header-dropdown-add' + (canAdd ? '' : ' disabled') + '" data-action="add-account"' + (canAdd ? '' : ' disabled') + '>+ Добавить вход</button>';
    dd.innerHTML = html;

    dd.querySelectorAll('.header-dropdown-account').forEach((el) => {
      const chatId = el.getAttribute('data-chat-id');
      el.addEventListener('click', (e) => {
        if (e.target.classList.contains('header-dropdown-account-remove')) return;
        setActiveChatId(chatId);
        closeAccountDropdown();
        loadMeAndShowCabinet();
      });
    });
    dd.querySelectorAll('.header-dropdown-account-remove').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const chatId = btn.getAttribute('data-chat-id');
        const sessions = getSessions().filter((s) => String(s.chat_id) !== String(chatId));
        const wasActive = String(getActiveChatId()) === String(chatId);
        setSessions(sessions);
        if (sessions.length) {
          if (wasActive) {
            setActiveChatId(sessions[0].chat_id);
            loadMeAndShowCabinet();
          }
        } else {
          setActiveChatId(null);
          renderHeader(null);
          showScreen('landing');
        }
        closeAccountDropdown();
      });
    });
    const addBtn = dd.querySelector('[data-action="add-account"]');
    if (addBtn && canAdd) {
      addBtn.addEventListener('click', () => {
        closeAccountDropdown();
        document.getElementById('login-modal')?.classList.remove('hidden');
      });
    }
    dd.classList.remove('hidden');
  }

  function renderHeader(me) {
    const header = document.getElementById('site-header');
    if (!header) return;
    const loginBtn = header.querySelector('[data-action="login"]');
    const userWrap = document.getElementById('header-user-wrap');
    const cabinetBtn = document.getElementById('header-cabinet-name');
    if (me && me.name) {
      if (loginBtn) loginBtn.classList.add('hidden');
      if (userWrap) userWrap.classList.remove('hidden');
      if (cabinetBtn) cabinetBtn.textContent = me.name;
    } else {
      if (loginBtn) loginBtn.classList.remove('hidden');
      if (userWrap) userWrap.classList.add('hidden');
      if (cabinetBtn) cabinetBtn.textContent = '';
    }
    closeAccountDropdown();
  }

  function showScreen(screenId) {
    ['landing', 'cabinet-readonly', 'cabinet-onboarding'].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.classList.add('hidden');
    });
    const target = document.getElementById(screenId);
    if (target) target.classList.remove('hidden');
  }

  function showSection(sectionId) {
    const readonly = document.getElementById('cabinet-readonly');
    const onboarding = document.getElementById('cabinet-onboarding');
    if (readonly && !readonly.classList.contains('hidden')) {
      readonly.querySelectorAll('.cabinet-section').forEach((el) => el.classList.add('hidden'));
      const t = readonly.querySelector('#section-' + sectionId);
      if (t) t.classList.remove('hidden');
      readonly.querySelectorAll('.cabinet-nav button').forEach((b) => {
        b.classList.remove('active');
        if (b.getAttribute('data-section') === sectionId) b.classList.add('active');
      });
      return;
    }
    if (onboarding && !onboarding.classList.contains('hidden')) {
      onboarding.querySelectorAll('.cabinet-section').forEach((el) => el.classList.add('hidden'));
      const t = onboarding.querySelector('#section-' + sectionId);
      if (t) t.classList.remove('hidden');
      onboarding.querySelectorAll('.cabinet-nav button').forEach((b) => {
        b.classList.remove('active');
        if (b.getAttribute('data-section') === sectionId) b.classList.add('active');
      });
    }
  }

  // ——— Вход по коду ———
  function bindLogin() {
    const modal = document.getElementById('login-modal');
    const openBtn = document.querySelector('[data-action="login"]');
    const closeBtn = document.querySelector('[data-action="close-login"]');
    const form = document.getElementById('login-form');
    const status = document.getElementById('login-status');

    if (openBtn) openBtn.addEventListener('click', () => modal && modal.classList.remove('hidden'));
    if (closeBtn) closeBtn.addEventListener('click', () => modal && modal.classList.add('hidden'));
    modal && modal.querySelector('.modal-backdrop') && modal.querySelector('.modal-backdrop').addEventListener('click', () => modal.classList.add('hidden'));

    if (form) {
      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const code = (form.code && form.code.value || '').trim().toUpperCase();
        if (!code) {
          if (status) { status.textContent = 'Введите код'; status.className = 'login-status error'; }
          return;
        }
        if (status) { status.textContent = 'Проверка...'; status.className = 'login-status'; }
        const data = await api('/api/site/validate', { method: 'POST', body: JSON.stringify({ code: code }) });
        if (data.success && data.token) {
          const sessions = getSessions();
          const isPersonal = !!data.is_personal;
          const chatId = String(data.chat_id);
          const existing = sessions.find((s) => String(s.chat_id) === chatId);
          if (existing) {
            existing.token = data.token;
            existing.name = data.name;
            existing.has_data = data.has_data;
            existing.is_personal = isPersonal;
            setSessions(sessions);
          } else {
            const personalCount = sessions.filter((s) => s.is_personal).length;
            const groupCount = sessions.filter((s) => !s.is_personal).length;
            if (isPersonal && personalCount >= MAX_PERSONAL) {
              if (status) { status.textContent = 'Максимум 2 личных кабинета'; status.className = 'login-status error'; }
              return;
            }
            if (!isPersonal && groupCount >= MAX_GROUP) {
              if (status) { status.textContent = 'Максимум 2 групповых кабинета'; status.className = 'login-status error'; }
              return;
            }
            sessions.push({ chat_id: chatId, token: data.token, name: data.name, has_data: data.has_data, is_personal: isPersonal });
            setSessions(sessions);
          }
          setActiveChatId(chatId);
          if (status) { status.textContent = 'Успешно!'; status.className = 'login-status success'; }
          setTimeout(() => {
            modal.classList.add('hidden');
            loadMeAndShowCabinet();
          }, 500);
        } else {
          if (status) { status.textContent = data.error || 'Неверный код'; status.className = 'login-status error'; }
        }
      });
    }
  }

  function loadMeAndShowCabinet() {
    api('/api/site/me').then((me) => {
      if (!me.success) {
        showScreen('landing');
        renderHeader(null);
        return;
      }
      cabinetHasData = !!me.has_data;
      renderHeader(me);
      loadExtensionConfig();
      if (me.has_data) {
        showScreen('cabinet-readonly');
        loadPlans();
        loadUnwatched();
        loadSeries();
        loadRatings();
      } else {
        showScreen('cabinet-onboarding');
      }
    });
  }

  function loadExtensionConfig() {
    fetch(API_BASE + '/api/site/config').then((r) => r.json()).then((data) => {
      if (!data.success || !data.chromeExtensionUrl) return;
      const ua = navigator.userAgent || '';
      const isOpera = /opr|opera/i.test(ua) || (navigator.browser && navigator.browser.opera);
      const url = isOpera ? (data.operaExtensionUrl || data.chromeExtensionUrl) : data.chromeExtensionUrl;
      document.querySelectorAll('#cabinet-extension-link, #cabinet-extension-link-onboard').forEach((a) => {
        if (a) { a.href = url; a.classList.remove('hidden'); }
      });
    }).catch(() => {});
  }

  // Перенос названия: до maxCh символов в строке, слово целиком; если слово длиннее — разбить
  function wrapTitleHtml(title, maxCh) {
    if (!title) return '';
    const safe = escapeHtml(title);
    if (safe.length <= maxCh) return safe;
    const parts = [];
    let rest = safe;
    while (rest.length > 0) {
      if (rest.length <= maxCh) {
        parts.push(rest);
        break;
      }
      const chunk = rest.slice(0, maxCh);
      const lastSpace = chunk.lastIndexOf(' ');
      const breakAt = lastSpace >= 0 ? lastSpace + 1 : maxCh;
      const word = rest.slice(0, breakAt);
      if (word.length > maxCh) {
        for (let i = 0; i < word.length; i += maxCh) parts.push(word.slice(i, i + maxCh));
      } else {
        parts.push(word);
      }
      rest = rest.slice(word.length);
    }
    return parts.map((p) => '<span class="plan-title-line">' + p + '</span>').join('');
  }

  // ——— Загрузка данных кабинета ———
  function loadPlans() {
    api('/api/site/plans').then((data) => {
      if (!data.success) return;
      const homeEl = document.getElementById('plans-home-list');
      const cinemaEl = document.getElementById('plans-cinema-list');
      const plansTodayEl = document.getElementById('plans-today');
      const renderPlan = (p) => {
        const dt = p.plan_datetime ? new Date(p.plan_datetime) : null;
        const dateLine = dt ? dt.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }) : '';
        const timeLine = dt ? dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }) : '';
        const typeLabel = p.plan_type === 'cinema' ? '🎥 В кино' : '🏠 Дома';
        const link = filmDeepLink(p.kp_id, p.is_series);
        const poster = posterUrl(p.kp_id);
        const titleSafe = escapeHtml(p.title || '');
        return `
          <div class="card plan-card">
            <div class="card-poster-wrap">
              ${poster ? '<img src="' + poster + '" alt="" class="card-poster" width="80" height="120" referrerpolicy="no-referrer" loading="lazy" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\'">' : ''}
              <div class="film-poster-placeholder" style="${poster ? 'display:none' : ''}">🎬</div>
            </div>
            <div class="plan-info">
              <div class="plan-meta">
                <span class="plan-date-line">📅 ${escapeHtml(dateLine)}</span>
                <span class="plan-time-line">${escapeHtml(timeLine)}</span>
                <span class="plan-type">${typeLabel}</span>
              </div>
              <div class="plan-title">🎬 ${titleSafe}</div>
            </div>
            <a href="${link}" target="_blank" rel="noopener" class="btn btn-small btn-primary">Открыть в Telegram</a>
          </div>`;
      };
      const homeEmpty = !data.home || !data.home.length;
      const cinemaEmpty = !data.cinema || !data.cinema.length;
      if (homeEl) {
        if (homeEmpty) {
          let html = '<p class="empty-hint">Нет планов просмотра дома.</p><div class="plans-empty-actions">';
          html += '<a href="' + BOT_SEARCH_LINK + '" target="_blank" rel="noopener" class="btn btn-small btn-primary">🔍 Найти фильмы в Боте</a>';
          if (cabinetHasData) html += ' <a href="' + BOT_RANDOM_LINK + '" target="_blank" rel="noopener" class="btn btn-small btn-secondary">🎲 Случайный фильм из базы</a>';
          html += '</div>';
          homeEl.innerHTML = html;
        } else {
          homeEl.innerHTML = data.home.map(renderPlan).join('');
        }
      }
      if (cinemaEl) {
        if (cinemaEmpty) {
          cinemaEl.innerHTML = '<p class="empty-hint">Нет планов в кино.</p><div class="plans-empty-actions"><a href="' + BOT_PREMIERES_LINK + '" target="_blank" rel="noopener" class="btn btn-small btn-primary">📆 Найти премьеры в Боте</a></div>';
        } else {
          cinemaEl.innerHTML = data.cinema.map(renderPlan).join('');
        }
      }
      const all = [...(data.home || []), ...(data.cinema || [])].slice(0, 3);
      const todayWrap = document.getElementById('plans-today-wrap');
      if (todayWrap) todayWrap.classList.toggle('hidden', !all.length);
      if (plansTodayEl) plansTodayEl.innerHTML = all.length ? all.map(renderPlan).join('') : '';
    });
  }

  let unwatchedItems = [];
  let unwatchedSortMode = 'date';

  function renderUnwatchedCard(m) {
    const link = filmDeepLink(m.kp_id, m.is_series);
    const year = m.year ? ` (${m.year})` : '';
    const poster = posterUrl(m.kp_id);
    const ratingStr = m.rating_kp != null ? ' · КП: ' + Number(m.rating_kp).toFixed(1) : '';
    const desc = (m.description || '').trim();
    const descHtml = desc ? '<div class="film-description">' + escapeHtml(desc.slice(0, 200)) + (desc.length > 200 ? '…' : '') + '</div>' : '';
    return `
      <a href="${link}" target="_blank" rel="noopener" class="card film-card">
        <div class="card-poster-wrap">
          ${poster ? '<img src="' + poster + '" alt="" class="card-poster" width="80" height="120" referrerpolicy="no-referrer" loading="lazy" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\'">' : ''}
          <div class="film-poster-placeholder" style="${poster ? 'display:none' : ''}">${m.is_series ? '📺' : '🎬'}</div>
        </div>
        <div class="film-info">
          <div class="film-title">${escapeHtml(m.title)}${year}${ratingStr}</div>
          ${descHtml}
          <div class="film-status">Статус: В базе</div>
          <span class="btn btn-small btn-primary">Продолжить в Telegram</span>
        </div>
      </a>`;
  }

  function renderUnwatchedList() {
    const el = document.getElementById('unwatched-list');
    if (!el) return;
    if (!unwatchedItems.length) {
      el.innerHTML = '<p class="empty-hint">Нет непросмотренных. Добавьте фильмы в боте.</p>';
      return;
    }
    let list = unwatchedItems.slice();
    if (unwatchedSortMode === 'date_old') list.reverse();
    if (unwatchedSortMode === 'az') list.sort((a, b) => (a.title || '').localeCompare(b.title || '', 'ru'));
    if (unwatchedSortMode === 'za') list.sort((a, b) => (b.title || '').localeCompare(a.title || '', 'ru'));
    el.innerHTML = list.map(renderUnwatchedCard).join('');
  }

  function loadUnwatched() {
    api('/api/site/unwatched').then((data) => {
      unwatchedItems = Array.isArray(data && data.items) ? data.items : [];
      const sortSelect = document.getElementById('unwatched-sort');
      if (sortSelect && !sortSelect.dataset.bound) {
        sortSelect.dataset.bound = '1';
        sortSelect.addEventListener('change', () => {
          unwatchedSortMode = sortSelect.value;
          renderUnwatchedList();
        });
      }
      if (sortSelect) sortSelect.value = unwatchedSortMode;
      renderUnwatchedList();
    }).catch(() => {
      unwatchedItems = [];
      renderUnwatchedList();
    });
  }

  function loadSeries() {
    api('/api/site/series').then((data) => {
      if (!data.success) return;
      const el = document.getElementById('series-list');
      if (!el) return;
      el.innerHTML = (data.items && data.items.length)
        ? data.items.map((s) => {
            const link = filmDeepLink(s.kp_id, true);
            const progress = s.progress ? `Прогресс: ${s.progress}` : 'Не начат';
            const poster = posterUrl(s.kp_id);
            return `
              <a href="${link}" target="_blank" rel="noopener" class="card series-card">
                <div class="card-poster-wrap">
                  ${poster ? '<img src="' + poster + '" alt="" class="card-poster" width="80" height="120" referrerpolicy="no-referrer" loading="lazy" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\'">' : ''}
                  <div class="film-poster-placeholder" style="${poster ? 'display:none' : ''}">📺</div>
                </div>
                <div class="film-info">
                  <div class="film-title">${escapeHtml(s.title)}</div>
                  <div class="film-status">${progress}</div>
                  <span class="btn btn-small btn-primary">Продолжить в Telegram</span>
                </div>
              </a>`;
          }).join('')
        : '<p class="empty-hint">Нет сериалов. Добавьте в боте.</p>';
    });
  }

  function loadRatings() {
    const el = document.getElementById('ratings-list');
    if (!el) return;
    api('/api/site/ratings').then((data) => {
      const items = Array.isArray(data && data.items) ? data.items : [];
      el.innerHTML = items.length
        ? items.map((r) => {
            const link = filmDeepLink(r.kp_id, false);
            const year = r.year ? ` (${r.year})` : '';
            const poster = posterUrl(r.kp_id);
            const ratingKpStr = r.rating_kp != null ? ' · КП: ' + Number(r.rating_kp).toFixed(1) : '';
            const desc = (r.description || '').trim();
            const descHtml = desc ? '<div class="film-description">' + escapeHtml(desc.slice(0, 200)) + (desc.length > 200 ? '…' : '') + '</div>' : '';
            const raterStr = (r.rater_username && r.rater_username.trim()) ? ' · ' + escapeHtml(r.rater_username.trim()) : '';
            return `
              <a href="${link}" target="_blank" rel="noopener" class="card film-card">
                <div class="card-poster-wrap">
                  ${poster ? '<img src="' + poster + '" alt="" class="card-poster" width="80" height="120" referrerpolicy="no-referrer" loading="lazy" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\'">' : ''}
                  <div class="film-poster-placeholder" style="${poster ? 'display:none' : ''}">⭐</div>
                </div>
                <div class="film-info">
                  <div class="film-title">${escapeHtml(r.title)}${year}${ratingKpStr}</div>
                  ${descHtml}
                  <div class="film-status">⭐ ${r.rating}${raterStr}</div>
                  <span class="btn btn-small btn-primary">Продолжить в Telegram</span>
                </div>
              </a>`;
          }).join('')
        : '<p class="empty-hint">Нет оценок.</p>';
    }).catch(() => {
      el.innerHTML = '<p class="empty-hint">Не удалось загрузить оценки.</p>';
    });
  }

  function escapeHtml(s) {
    if (!s) return '';
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  // ——— FAQ аккордеон ———
  function bindFaq() {
    document.querySelectorAll('.faq-item').forEach((item) => {
      const head = item.querySelector('.faq-head');
      if (!head) return;
      head.addEventListener('click', () => item.classList.toggle('open'));
    });
  }

  // ——— Инициализация ———
  function init() {
    bindLogin();
    bindFaq();

    document.querySelectorAll('.cabinet-nav [data-section]').forEach((btn) => {
      btn.addEventListener('click', () => showSection(btn.getAttribute('data-section')));
    });

    const cabinetName = document.getElementById('header-cabinet-name');
    const userWrap = document.getElementById('header-user-wrap');
    if (cabinetName) {
      cabinetName.addEventListener('click', (e) => {
        e.preventDefault();
        const dd = document.getElementById('header-account-dropdown');
        if (dd && dd.classList.contains('hidden')) openAccountDropdown();
        else closeAccountDropdown();
      });
    }
    document.addEventListener('click', (e) => {
      if (userWrap && !userWrap.contains(e.target)) closeAccountDropdown();
    });

    window.addEventListener('mp:logout', () => {
      renderHeader(null);
      showScreen('landing');
    });

    // Миграция: старый одиночный токен -> одна сессия в списке
    const oldToken = localStorage.getItem('mp_site_token');
    if (!getToken() && oldToken) {
      const headers = { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + oldToken };
      fetch(API_BASE + '/api/site/me', { headers })
        .then((r) => r.json().catch(() => ({})))
        .then((me) => {
          if (me.success && me.chat_id != null) {
            const session = {
              chat_id: String(me.chat_id),
              token: oldToken,
              name: me.name,
              has_data: !!me.has_data,
              is_personal: !!me.is_personal
            };
            setSessions([session]);
            setActiveChatId(session.chat_id);
            localStorage.removeItem('mp_site_token');
            loadMeAndShowCabinet();
          } else {
            localStorage.removeItem('mp_site_token');
            showScreen('landing');
            renderHeader(null);
          }
        })
        .catch(() => {
          localStorage.removeItem('mp_site_token');
          showScreen('landing');
          renderHeader(null);
        });
      return;
    }

    if (getToken()) {
      loadMeAndShowCabinet();
    } else {
      showScreen('landing');
      renderHeader(null);
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
