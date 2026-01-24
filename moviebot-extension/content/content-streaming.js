// content-streaming.js
// Content script для стриминговых платформ: отслеживание просмотра фильмов и сериалов

(function() {
  'use strict';
  
  const API_BASE_URL = 'https://web-production-3921c.up.railway.app';
  
  // ────────────────────────────────────────────────
  // Вспомогательная функция для API запросов через background script
  // ────────────────────────────────────────────────
  async function apiRequest(method, endpoint, body = null) {
    try {
      const url = `${API_BASE_URL}${endpoint}`;
      const message = {
        action: 'streaming_api_request',
        method: method,
        url: url,
        headers: { 'Content-Type': 'application/json' },
        body: body
      };
      
      return new Promise((resolve, reject) => {
        chrome.runtime.sendMessage(message, (response) => {
          if (chrome.runtime.lastError) {
            console.error('[STREAMING] chrome.runtime.lastError:', chrome.runtime.lastError);
            reject(new Error(chrome.runtime.lastError.message));
            return;
          }
          
          if (!response) {
            console.error('[STREAMING] Нет ответа от background script');
            reject(new Error('No response from background script'));
            return;
          }
          
          console.log('[STREAMING] Ответ от background script:', response);
          
          if (!response.success) {
            console.error('[STREAMING] Ошибка в ответе:', response.error);
            reject(new Error(response.error || 'Unknown error'));
            return;
          }
          
          resolve({
            ok: response.status >= 200 && response.status < 300,
            status: response.status,
            json: async () => response.data
          });
        });
      });
    } catch (error) {
      console.error('[STREAMING] Ошибка apiRequest:', error);
      throw error;
    }
  }
  
  // Поддерживаемые сайты
  const supportedHosts = [
    'tvoe.live', 'ivi.ru', 'okko.tv', 'kinopoisk.ru', 'hd.kinopoisk.ru',
    'premier.one', 'wink.ru', 'start.ru', 'amediateka.ru',
    'rezka.ag', 'rezka.ad', 'hdrezka', 'lordfilm', 'allserial', 'boxserial'
  ];
  
  const hostname = window.location.hostname.toLowerCase();
  if (!supportedHosts.some(h => hostname.includes(h))) {
    return; // Сайт не поддерживается
  }
  
  // ────────────────────────────────────────────────
  // Конфигурации парсинга для каждого сайта
  // ────────────────────────────────────────────────
  const siteConfigs = {
    'tvoe.live': {
      isSeries: () => {
        const meta = document.querySelector('meta[name="description"]');
        return meta?.content?.includes('сериал') || false;
      },
      title: {
        selector: 'meta[name="description"]',
        extract: (el) => {
          const c = el?.content || '';
          const m = c.match(/Смотрите (?:сериал|фильм)\s+([^(\n]+?)\s*\(/i);
          return m ? m[1].trim() : null;
        }
      },
      year: {
        selector: 'meta[name="description"]',
        extract: (el) => el?.content?.match(/\((20\d{2})\)/)?.[1]
      },
      seasonEpisode: {
        selector: '#player-container div.VideoJS_titleWrapper__RPVJ7 > p.VideoJS_desc__kaIbK, p[class*="VideoJS_desc"], #trailerCard button div, .MovieCard_content__3a8LO button div',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон[,\s]+(\d+)\s*серия/i) || t.match(/Продолжить\s+(\d+)\s*сезон[,\s]+(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      }
    },
    
    'ivi.ru': {
      isSeries: () => {
        if (document.querySelector('.postersListDesktop__seasonTitle') || 
            document.querySelector('.serieBadge')) {
          return true;
        }
        // Проверяем breadcrumbs через JavaScript (не через :contains, т.к. это не валидный CSS)
        const breadcrumbs = document.querySelectorAll('#root .breadCrumbs__item');
        for (const item of breadcrumbs) {
          if (item.textContent?.includes('Сериалы')) {
            return true;
          }
        }
        return false;
      },
      title: {
        selector: 'title, meta[property="og:title"]',
        extract: (el) => {
          const text = el?.textContent || el?.content || '';
          // Убираем лишний текст типа "Сериал ... смотреть онлайн все серии подряд в хорошем HD качестве"
          let cleanTitle = text.split(/[:|]/)[0]?.trim() || '';
          // Убираем "Сериал" в начале, если есть
          cleanTitle = cleanTitle.replace(/^Сериал\s+/i, '');
          // Убираем все после "смотреть" или "в хорошем"
          cleanTitle = cleanTitle.split(/\s+смотреть/i)[0]?.trim() || cleanTitle;
          cleanTitle = cleanTitle.split(/\s+в хорошем/i)[0]?.trim() || cleanTitle;
          // Убираем сезон/серию из названия (например "1 сезон 2 серия")
          cleanTitle = cleanTitle.replace(/\s+\d+\s*сезон\s*\d+\s*серия/i, '').trim();
          cleanTitle = cleanTitle.replace(/\s+\d+\s*сезон/i, '').trim();
          cleanTitle = cleanTitle.replace(/\s+\d+\s*серия/i, '').trim();
          // Убираем год в конце, если он есть (он будет в отдельном поле)
          cleanTitle = cleanTitle.replace(/\s+\d{4}\s*$/, '').trim();
          return cleanTitle || null;
        }
      },
      year: {
        selector: '.paramsList__container a[href*="/movies/"], .paramsList__container a[href*="/series/"]',
        extract: (el) => el?.textContent?.trim() || null
      },
      seasonEpisode: {
        selector: '.postersListDesktop__listTitle span, .serieBadge button div, .nbl-button__primaryText',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i) || t.match(/Серия (\d+) сезон (\d+)/i);
          return m ? { season: parseInt(m[2] || m[1]), episode: parseInt(m[1] || m[2]) } : null;
        }
      }
    },
    
    'okko.tv': {
      isSeries: () => {
        const title = document.querySelector('title');
        return title?.textContent?.includes('сезон') || title?.textContent?.includes('серии') || false;
      },
      title: {
        selector: 'title',
        extract: (el) => el?.textContent?.split(/[\(\[]/)[0]?.trim() || null
      },
      year: {
        selector: 'span[test-id="meta_release_date"]',
        extract: (el) => el?.textContent?.split('-')[0]?.trim()
      },
      seasonEpisode: {
        selector: '[test-id="player_content_title"], h4[test-id="content_progress_title"]',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      }
    },
    
    'kinopoisk.ru,hd.kinopoisk.ru': {
      isSeries: () => {
        const title = document.querySelector('title[data-tid="HdSeoHead"], title');
        return title?.textContent?.includes('(сериал') || false;
      },
      title: {
        selector: 'title[data-tid="HdSeoHead"], title',
        extract: (el) => {
          const text = el?.textContent || '';
          return text.split(/[,（(]/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'title',
        extract: (el) => el?.textContent?.match(/(\d{4})/)?.[1]
      },
      seasonEpisode: {
        selector: '.styles_subtitle__PPaVH, .styles_extraInfo__A3zOn div',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      }
    },
    
    'premier.one': {
      isSeries: () => {
        const meta = document.querySelector('meta[property="og:title"]');
        return meta?.content?.includes('сериал') || false;
      },
      title: {
        selector: 'meta[property="og:title"]',
        extract: (el) => {
          const content = el?.content || '';
          return content.split(/сериал|фильм/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'meta[property="og:title"]',
        extract: (el) => el?.content?.match(/(\d{4})/)?.[1]
      },
      seasonEpisode: {
        selector: 'p.header-module_subtitle__xeHTB',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        },
        fromUrl: () => {
          const path = window.location.pathname;
          const seasonMatch = path.match(/season\/(\d+)/);
          const episodeMatch = path.match(/episode\/(\d+)/);
          if (seasonMatch && episodeMatch) {
            return { season: parseInt(seasonMatch[1]), episode: parseInt(episodeMatch[1]) };
          }
          return null;
        }
      }
    },
    
    'wink.ru': {
      isSeries: () => {
        const title = document.querySelector('title');
        return title?.textContent?.includes('сериал') || false;
      },
      title: {
        selector: 'title',
        extract: (el) => {
          const text = el?.textContent || '';
          return text.replace(/Плеер (?:сериал|фильм) /, '').split(/[,（(]/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'title',
        extract: (el) => el?.textContent?.match(/(\d{4})/)?.[1]
      },
      seasonEpisode: {
        fromUrl: () => {
          const path = window.location.pathname;
          const seasonMatch = path.match(/sezon-(\d+)/);
          const episodeMatch = path.match(/seriya-(\d+)/);
          if (seasonMatch && episodeMatch) {
            return { season: parseInt(seasonMatch[1]), episode: parseInt(episodeMatch[1]) };
          }
          return null;
        }
      }
    },
    
    'start.ru': {
      isSeries: () => {
        const title = document.querySelector('title');
        return title?.textContent?.includes('серии') || false;
      },
      title: {
        selector: 'title',
        extract: (el) => el?.textContent?.split(/смотреть|▹/)[0]?.trim() || null
      },
      year: {
        selector: 'title',
        extract: (el) => {
          const text = el?.textContent || '';
          const match = text.match(/(\d{4})/);
          return match ? match[1] : null;
        }
      },
      seasonEpisode: {
        selector: '.StartPlayer_title__4d3nF',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      }
    },
    
    'amediateka.ru': {
      isSeries: () => {
        const title = document.querySelector('title[data-next-head], title');
        return title?.textContent?.includes('Сериал') || false;
      },
      title: {
        selector: 'title[data-next-head], title',
        extract: (el) => {
          const text = el?.textContent || '';
          return text.replace(/^(Сериал|Фильм)\s+/, '').split(/смотреть/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'title',
        extract: (el) => el?.textContent?.match(/(\d{4})/)?.[1]
      },
      seasonEpisode: {
        selector: '.PlayButton_playButtonContext__4XH_C, .PlayerData_episodeInfo__D7dT7',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          // Формат: "1 сезон, 1 серия" или "1 сезон,  1 серия" (с двойным пробелом)
          const m = t.match(/(\d+)\s*сезон\s*,?\s*(\d+)\s*серия/i);
          if (m) {
            return { season: parseInt(m[1]), episode: parseInt(m[2]) };
          }
          return null;
        }
      }
    },
    
    'rezka,hdrezka': {
      isSeries: () => {
        const h1 = document.querySelector('h1.full-article__title');
        return h1?.textContent?.includes('сезон') || h1?.textContent?.includes('серия') || false;
      },
      title: {
        selector: 'h1.full-article__title',
        extract: (el) => {
          const text = el?.textContent || '';
          return text.split(/\d{4}|сезон|серия/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'h1.full-article__title span',
        extract: (el) => el?.textContent?.match(/\d{4}/)?.[0]
      },
      seasonEpisode: {
        selector: '.headText_3i3, .select__item-text',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const s = t.match(/Сезон\s*(\d+)/i)?.[1];
          const e = t.match(/Серия\s*(\d+)/i)?.[1];
          if (s || e) {
            return { season: s ? parseInt(s) : null, episode: e ? parseInt(e) : null };
          }
          return null;
        }
      }
    },
    
    'lordfilm': {
      isSeries: () => {
        const breadcrumb = document.querySelector('#dle-speedbar');
        return breadcrumb?.textContent?.includes('Сериалы') || false;
      },
      title: {
        selector: '#dle-speedbar span[itemprop="name"]:last-child',
        extract: (el) => {
          const text = el?.textContent?.trim() || '';
          return text.replace(/\s*\(\d{4}\)$/, '').trim() || null;
        }
      },
      year: {
        selector: '#dle-speedbar span[itemprop="name"]:last-child',
        extract: (el) => el?.textContent?.match(/\d{4}/)?.[0]
      },
      seasonEpisode: {
        selector: '.headText_3i3, .select__item-text, .item-el.item-st',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const s = t.match(/Сезон\s*(\d+)/i)?.[1] || t.match(/(\d+)\s*сезон/i)?.[1];
          const e = t.match(/(\d+)\s*серия/i)?.[1];
          if (s || e) {
            return { season: s ? parseInt(s) : null, episode: e ? parseInt(e) : null };
          }
          return null;
        }
      }
    },
    
    'allserial': {
      isSeries: () => true, // На этом сайте только сериалы
      title: {
        selector: 'h1.short-title',
        extract: (el) => el?.textContent?.split(/\d+\s*сезон/)[0]?.trim() || null
      },
      year: {
        selector: 'span[itemprop="datePublished"]',
        extract: (el) => el?.textContent?.trim()
      },
      seasonEpisode: {
        selector: '.jq-selectbox__select-text span',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const s = t.match(/(\d+)\s*сезон/i)?.[1];
          const e = t.match(/(\d+)\s*серия/i)?.[1];
          if (s || e) {
            return { season: s ? parseInt(s) : null, episode: e ? parseInt(e) : null };
          }
          return null;
        }
      }
    },
    
    'boxserial': {
      isSeries: () => true, // На этом сайте только сериалы
      title: {
        selector: '.page__titles h1',
        extract: (el) => el?.textContent?.split(/1,2,3|сезон/)[0]?.trim() || null
      },
      year: {
        selector: 'ul.page__info li:nth-child(1) span:nth-child(2)',
        extract: (el) => el?.textContent?.trim()
      },
      seasonEpisode: {
        selector: '[data-v-dac944a7], .headText_3i3, .select__item-text',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const s = t.match(/Сезон\s*(\d+)/i)?.[1];
          const e = t.match(/(Эпизод|серия)\s*(\d+)/i)?.[2];
          if (s || e) {
            return { season: s ? parseInt(s) : null, episode: e ? parseInt(e) : null };
          }
          return null;
        }
      }
    }
  };
  
  // ────────────────────────────────────────────────
  // Получение конфигурации для текущего сайта
  // ────────────────────────────────────────────────
  function getSiteConfig() {
    for (const [key, config] of Object.entries(siteConfigs)) {
      const hosts = key.split(',');
      if (hosts.some(h => hostname.includes(h.trim()))) {
        return config;
      }
    }
    return null;
  }
  
  // ────────────────────────────────────────────────
  // Парсинг информации о контенте
  // ────────────────────────────────────────────────
  function getContentInfo() {
    const config = getSiteConfig();
    if (!config) return null;
    
    let title = null;
    let year = null;
    let seasonEpisode = null;
    let isSeries = false;
    
    // Парсинг названия
    if (config.title?.selector) {
      const el = document.querySelector(config.title.selector);
      if (el && config.title.extract) {
        title = config.title.extract(el);
      }
    }
    
    // Парсинг года
    if (config.year?.selector) {
      const el = document.querySelector(config.year.selector);
      if (el && config.year.extract) {
        year = config.year.extract(el);
      }
    }
    
    // Парсинг сезона/серии
    if (config.seasonEpisode) {
      if (config.seasonEpisode.fromUrl) {
        seasonEpisode = config.seasonEpisode.fromUrl();
      }
      if (!seasonEpisode && config.seasonEpisode.selector) {
        const el = document.querySelector(config.seasonEpisode.selector);
        if (el && config.seasonEpisode.extract) {
          seasonEpisode = config.seasonEpisode.extract(el);
        }
      }
    }
    
    // Определение типа (сериал/фильм)
    if (typeof config.isSeries === 'function') {
      isSeries = config.isSeries();
    } else {
      isSeries = config.isSeries || !!seasonEpisode;
    }
    
    return {
      title: title || document.title.split(/[-|]/)[0].trim(),
      year: year,
      season: seasonEpisode?.season || null,
      episode: seasonEpisode?.episode || null,
      isSeries: isSeries,
      url: window.location.href
    };
  }
  
  // ────────────────────────────────────────────────
  // Защита от спама и кэширование
  // ────────────────────────────────────────────────
  let lastShown = {};
  let lastContentKey = '';
  let debounceTimer = null;
  let checkInterval = null;
  let lastUrl = location.href;
  let lastContentHash = '';
  
  // Кэш локальных данных (последние 100 просмотров)
  const CACHE_KEY = 'movieplanner_streaming_cache';
  const MAX_CACHE_SIZE = 100;
  
  async function getLocalCache() {
    try {
      const data = await chrome.storage.local.get([CACHE_KEY]);
      return data[CACHE_KEY] || [];
    } catch (e) {
      console.error('[STREAMING] Ошибка получения кэша:', e);
      return [];
    }
  }
  
  async function saveToLocalCache(info, kpId) {
    try {
      const cache = await getLocalCache();
      // Добавляем в начало
      cache.unshift({ title: info.title, year: info.year, kp_id: kpId, timestamp: Date.now() });
      // Оставляем только последние MAX_CACHE_SIZE записей
      if (cache.length > MAX_CACHE_SIZE) {
        cache.splice(MAX_CACHE_SIZE);
      }
      await chrome.storage.local.set({ [CACHE_KEY]: cache });
    } catch (e) {
      console.error('[STREAMING] Ошибка сохранения в кэш:', e);
    }
  }
  
  async function findInLocalCache(info) {
    try {
      const cache = await getLocalCache();
      const match = cache.find(item => 
        item.title?.toLowerCase() === info.title?.toLowerCase() && 
        item.year === info.year
      );
      return match?.kp_id || null;
    } catch (e) {
      console.error('[STREAMING] Ошибка поиска в кэше:', e);
      return null;
    }
  }
  
  function getContentKey(info) {
    return `${info.title}|${info.year}|${info.season || ''}|${info.episode || ''}`;
  }
  
  function getContentHash(info) {
    return `${info.title}_${info.year}_${info.season || ''}_${info.episode || ''}`;
  }
  
  function shouldShowOverlay(info) {
    const key = getContentKey(info);
    const hash = getContentHash(info);
    
    // Если тот же контент (включая сезон/серию), не показываем
    if (hash === lastContentHash) {
      console.log('[STREAMING] Пропуск: тот же контент (hash совпадает)');
      return false;
    }
    
    // Если изменился сезон или серия, сбрасываем кулдаун для этого фильма
    const now = Date.now();
    const last = lastShown[key] || 0;
    const timeSinceLastShow = now - last;
    
    // Если изменился сезон/серия, показываем сразу (не ждем кулдаун)
    const isNewEpisode = info.season && info.episode && 
                         (lastContentHash && !hash.startsWith(lastContentHash.split('_').slice(0, 3).join('_')));
    
    if (!isNewEpisode && timeSinceLastShow < 3 * 60 * 1000) {
      console.log('[STREAMING] Пропуск: кулдаун активен (прошло', Math.round(timeSinceLastShow / 1000), 'сек)');
      return false;
    }
    
    // Обновляем ключи и время показа
    lastContentKey = key;
    lastShown[key] = now;
    lastContentHash = hash;
    console.log('[STREAMING] Разрешено показать overlay: новый hash =', hash);
    return true;
  }
  
  // ────────────────────────────────────────────────
  // Плавающая плашка с кнопками (перетаскиваемая)
  // ────────────────────────────────────────────────
  let overlayElement = null;
  let currentInfo = null;
  let currentKpId = null;
  let currentFilmId = null;
  let currentFilmData = null;
  
  // Функция загрузки позиции плашки из localStorage
  function loadOverlayPosition() {
    try {
      const saved = localStorage.getItem('movieplanner_streaming_overlay_position');
      if (saved) {
        const pos = JSON.parse(saved);
        // Возвращаем только валидные числовые значения
        const result = {};
        if (typeof pos.top === 'number' && !isNaN(pos.top)) result.top = pos.top;
        if (typeof pos.right === 'number' && !isNaN(pos.right)) result.right = pos.right;
        if (typeof pos.left === 'number' && !isNaN(pos.left)) result.left = pos.left;
        if (typeof pos.bottom === 'number' && !isNaN(pos.bottom)) result.bottom = pos.bottom;
        return result;
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка загрузки позиции плашки:', e);
    }
    return {}; // Возвращаем пустой объект, чтобы использовались значения по умолчанию
  }
  
  // Функция сохранения позиции плашки в localStorage
  function saveOverlayPosition(position) {
    try {
      localStorage.setItem('movieplanner_streaming_overlay_position', JSON.stringify(position));
    } catch (e) {
      console.error('[STREAMING] Ошибка сохранения позиции плашки:', e);
    }
  }
  
  function removeOverlay() {
    if (overlayElement) {
      overlayElement.remove();
      overlayElement = null;
    }
  }
  
  async function createOverlay(info, filmData) {
    removeOverlay();
    
    currentInfo = info;
    currentFilmData = filmData;
    currentKpId = filmData?.kp_id || null;
    currentFilmId = filmData?.film_id || null;
    
    overlayElement = document.createElement('div');
    overlayElement.id = 'movieplanner-streaming-overlay';
    
    // Загружаем сохраненную позицию
    const savedPos = loadOverlayPosition();
    console.log('[STREAMING] Загруженная позиция:', savedPos);
    
    // Устанавливаем начальную позицию
    let initialStyle = `
      position: fixed;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 16px;
      border-radius: 12px;
      z-index: 999998;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
      max-width: 320px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 14px;
      line-height: 1.4;
      pointer-events: auto;
      cursor: move;
      user-select: none;
      display: block !important;
      visibility: visible !important;
      opacity: 1 !important;
    `;
    
    // Устанавливаем позицию (обязательно указываем и left/right, и top/bottom)
    // Проверяем, что значения валидные (не null, не undefined, не NaN)
    if (savedPos && typeof savedPos.left === 'number' && !isNaN(savedPos.left)) {
      initialStyle += `left: ${savedPos.left}px !important; right: auto !important;`;
      console.log('[STREAMING] Установлена позиция left:', savedPos.left);
    } else if (savedPos && typeof savedPos.right === 'number' && !isNaN(savedPos.right)) {
      initialStyle += `right: ${savedPos.right}px !important; left: auto !important;`;
      console.log('[STREAMING] Установлена позиция right:', savedPos.right);
    } else {
      initialStyle += `right: 20px !important; left: auto !important;`;
      console.log('[STREAMING] Установлена позиция по умолчанию: right 20px');
    }
    
    if (savedPos && typeof savedPos.top === 'number' && !isNaN(savedPos.top)) {
      initialStyle += `top: ${savedPos.top}px !important; bottom: auto !important;`;
      console.log('[STREAMING] Установлена позиция top:', savedPos.top);
    } else if (savedPos && typeof savedPos.bottom === 'number' && !isNaN(savedPos.bottom)) {
      initialStyle += `bottom: ${savedPos.bottom}px !important; top: auto !important;`;
      console.log('[STREAMING] Установлена позиция bottom:', savedPos.bottom);
    } else {
      initialStyle += `bottom: 20px !important; top: auto !important;`;
      console.log('[STREAMING] Установлена позиция по умолчанию: bottom 20px');
    }
    
    overlayElement.style.cssText = initialStyle;
    
    const titleText = info.isSeries 
      ? `${info.title} ${info.year ? `(${info.year})` : ''} - ${info.season || '?'} сезон, ${info.episode || '?'} серия`
      : `${info.title} ${info.year ? `(${info.year})` : ''}`;
    
    overlayElement.innerHTML = `
      <div style="margin-bottom: 12px;">
        <strong style="font-size: 16px;">🎬 Movie Planner</strong>
        <div style="margin-top: 8px; opacity: 0.9;">${titleText}</div>
      </div>
      <div id="mpp-buttons-container"></div>
      <button id="mpp-close" style="position: absolute; top: 8px; right: 8px; background: rgba(255,255,255,0.2); border: none; color: white; width: 24px; height: 24px; border-radius: 50%; cursor: pointer; font-size: 18px; line-height: 1;">×</button>
    `;
    
    document.body.appendChild(overlayElement);
    console.log('[STREAMING] Overlay добавлен в DOM:', overlayElement);
    
    // Кнопка закрытия
    overlayElement.querySelector('#mpp-close').addEventListener('click', (e) => {
      e.stopPropagation();
      removeOverlay();
    });
    
    // Перетаскивание плашки
    let isDragging = false;
    let dragStartX = 0;
    let dragStartY = 0;
    let initialLeft = 0;
    let initialTop = 0;
    let hasMoved = false;
    
    overlayElement.addEventListener('mousedown', (e) => {
      // Игнорируем клики на кнопки и контейнер кнопок
      if (e.target.closest('button') || e.target.closest('#mpp-buttons-container') || e.target.id === 'mpp-close') {
        return;
      }
      
      if (e.button !== 0) return; // Только левая кнопка мыши
      
      dragStartX = e.clientX;
      dragStartY = e.clientY;
      hasMoved = false;
      
      // Получаем текущую позицию плашки
      const rect = overlayElement.getBoundingClientRect();
      initialLeft = rect.left;
      initialTop = rect.top;
      
      e.preventDefault();
    });
    
    document.addEventListener('mousemove', (e) => {
      if (dragStartX === 0 && dragStartY === 0) return;
      
      const deltaX = Math.abs(e.clientX - dragStartX);
      const deltaY = Math.abs(e.clientY - dragStartY);
      
      // Если мышь сдвинулась больше чем на 5px, начинаем перетаскивание
      if (deltaX > 5 || deltaY > 5) {
        hasMoved = true;
        if (!isDragging) {
          isDragging = true;
          overlayElement.style.cursor = 'grabbing';
          overlayElement.style.transition = 'none';
        }
        
        const newLeft = initialLeft + (e.clientX - dragStartX);
        const newTop = initialTop + (e.clientY - dragStartY);
        
        // Ограничиваем перемещение в пределах окна
        const maxLeft = window.innerWidth - overlayElement.offsetWidth;
        const maxTop = window.innerHeight - overlayElement.offsetHeight;
        
        const clampedLeft = Math.max(0, Math.min(newLeft, maxLeft));
        const clampedTop = Math.max(0, Math.min(newTop, maxTop));
        
        overlayElement.style.left = `${clampedLeft}px`;
        overlayElement.style.top = `${clampedTop}px`;
        overlayElement.style.right = 'auto';
        overlayElement.style.bottom = 'auto';
      }
    });
    
    document.addEventListener('mouseup', () => {
      if (isDragging && hasMoved) {
        // Сохраняем позицию
        const rect = overlayElement.getBoundingClientRect();
        saveOverlayPosition({
          left: rect.left,
          top: rect.top,
          right: null,
          bottom: null
        });
      }
      
      isDragging = false;
      hasMoved = false;
      dragStartX = 0;
      dragStartY = 0;
      
      if (overlayElement) {
        overlayElement.style.cursor = 'move';
        overlayElement.style.transition = '';
      }
    });
    
    // Рендерим кнопки
    await renderButtons(info, filmData);
    console.log('[STREAMING] createOverlay завершен, renderButtons вызван');
  }
  
  async function renderButtons(info, filmData) {
    console.log('[STREAMING] renderButtons вызван с данными:', { info, filmData });
    const container = overlayElement?.querySelector('#mpp-buttons-container');
    if (!container) {
      console.error('[STREAMING] renderButtons: контейнер не найден!', overlayElement);
      return;
    }
    
    console.log('[STREAMING] renderButtons: контейнер найден, очищаем');
    container.innerHTML = '';
    
    // ВАЖНО: проверяем film_id явно (может быть 0, null, undefined)
    // undefined означает "неизвестно" (ошибка API), null означает "точно нет в базе"
    const filmId = filmData?.film_id;
    const isInDatabase = filmId !== null && filmId !== undefined;
    const isUnknown = filmId === undefined; // Не знаем статус из-за ошибки API
    
    console.log('[STREAMING] renderButtons: isInDatabase=', isInDatabase, 'isUnknown=', isUnknown, 'film_id=', filmId);
    
    // Если статус неизвестен (ошибка API), но есть kp_id - показываем кнопки для отметки
    // Предполагаем, что фильм может быть в базе
    if (isUnknown && filmData?.kp_id) {
      // Показываем кнопки для отметки, но не кнопку "Добавить в базу"
      if (info.isSeries) {
        const storageData = await chrome.storage.local.get(['has_notifications_access']);
        const hasNotificationsAccess = storageData.has_notifications_access || false;
        
        if (!hasNotificationsAccess) {
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px; background: rgba(255,255,255,0.1); border-radius: 6px; text-align: center; font-size: 13px; margin-bottom: 8px;';
          noAccessMsg.innerHTML = '🔒 Для отметки серий нужна подписка "Уведомления" или "Пакетная"<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else if (!filmData.current_episode_watched) {
          const markCurrentBtn = document.createElement('button');
          markCurrentBtn.textContent = `✅ Отметить серию ${info.season || '?'}×${info.episode || '?'}`;
          markCurrentBtn.style.cssText = `
            width: 100%;
            padding: 10px;
            background: white;
            color: #667eea;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            margin-bottom: 8px;
          `;
          markCurrentBtn.addEventListener('click', () => handleMarkEpisode(info, filmData, false));
          container.appendChild(markCurrentBtn);
        }
      }
      return;
    }
    
    if (!isInDatabase) {
      // Фильм/сериал не в базе - показываем кнопку "Добавить в базу"
      const addBtn = document.createElement('button');
      addBtn.textContent = '➕ Добавить в базу';
      addBtn.style.cssText = `
        width: 100%;
        padding: 10px;
        background: white;
        color: #667eea;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        cursor: pointer;
        margin-bottom: 8px;
      `;
      addBtn.addEventListener('click', () => handleAddToDatabase(info, filmData));
      container.appendChild(addBtn);
    } else {
      // Фильм/сериал в базе - проверяем подписку
      const storageData = await chrome.storage.local.get(['has_notifications_access']);
      const hasNotificationsAccess = storageData.has_notifications_access || false;
      
      if (info.isSeries) {
        // Сериал
        if (!hasNotificationsAccess) {
          // Нет подписки - показываем только информацию
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px; background: rgba(255,255,255,0.1); border-radius: 6px; text-align: center; font-size: 13px; margin-bottom: 8px;';
          noAccessMsg.innerHTML = '🔒 Для отметки серий нужна подписка "Уведомления" или "Пакетная"<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else {
          if (!filmData.current_episode_watched) {
            const markCurrentBtn = document.createElement('button');
            markCurrentBtn.textContent = `✅ Отметить серию ${info.season || '?'}×${info.episode || '?'}`;
            markCurrentBtn.style.cssText = `
              width: 100%;
              padding: 10px;
              background: white;
              color: #667eea;
              border: none;
              border-radius: 6px;
              font-weight: 600;
              cursor: pointer;
              margin-bottom: 8px;
            `;
            markCurrentBtn.addEventListener('click', () => handleMarkEpisode(info, filmData, false));
            container.appendChild(markCurrentBtn);
          }
          if (info.season && info.episode && filmData.has_unwatched_before) {
            const markAllBtn = document.createElement('button');
            markAllBtn.textContent = '✅ Отметить все предыдущие';
            markAllBtn.style.cssText = `
              width: 100%;
              padding: 10px;
              background: rgba(255,255,255,0.2);
              color: white;
              border: 1px solid rgba(255,255,255,0.3);
              border-radius: 6px;
              font-weight: 600;
              cursor: pointer;
              margin-bottom: 8px;
            `;
            markAllBtn.addEventListener('click', () => handleMarkEpisode(info, filmData, true));
            container.appendChild(markAllBtn);
          }
        }
      } else {
        // Фильм
        if (!hasNotificationsAccess) {
          // Нет подписки - показываем только информацию
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px; background: rgba(255,255,255,0.1); border-radius: 6px; text-align: center; font-size: 13px; margin-bottom: 8px;';
          noAccessMsg.innerHTML = '🔒 Для отметки фильмов нужна подписка "Уведомления" или "Пакетная"<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else {
          // Есть подписка - показываем кнопки
          if (!filmData.watched) {
            const markWatchedBtn = document.createElement('button');
            markWatchedBtn.textContent = '✅ Отметить как просмотренный';
            markWatchedBtn.style.cssText = `
              width: 100%;
              padding: 10px;
              background: white;
              color: #667eea;
              border: none;
              border-radius: 6px;
              font-weight: 600;
              cursor: pointer;
              margin-bottom: 8px;
            `;
            markWatchedBtn.addEventListener('click', () => handleMarkFilmWatched(info, filmData));
            container.appendChild(markWatchedBtn);
          } else if (!filmData.rated) {
            // Фильм просмотрен, но не оценен - показываем оценку
            showRatingButtons(info, filmData);
          }
        }
      }
    }
  }
  
  function showRatingButtons(info, filmData) {
    const container = overlayElement.querySelector('#mpp-buttons-container');
    if (!container) return;
    
    container.innerHTML = '<div style="margin-bottom: 8px; font-weight: 600;">Оцените фильм:</div>';
    
    const ratingContainer = document.createElement('div');
    ratingContainer.style.cssText = 'display: flex; gap: 4px; flex-wrap: wrap; margin-bottom: 8px;';
    
    for (let i = 1; i <= 10; i++) {
      const btn = document.createElement('button');
      btn.textContent = '⭐';
      btn.dataset.rating = i;
      btn.style.cssText = `
        flex: 1;
        min-width: 28px;
        height: 36px;
        background: rgba(255,255,255,0.2);
        color: white;
        border: 1px solid rgba(255,255,255,0.3);
        border-radius: 6px;
        cursor: pointer;
        font-size: 16px;
        transition: all 0.2s;
      `;
      
      btn.addEventListener('click', () => handleRating(info, filmData, i));
      btn.addEventListener('mouseenter', () => {
        highlightRating(i);
      });
      
      ratingContainer.appendChild(btn);
    }
    
    container.appendChild(ratingContainer);
  }
  
  function highlightRating(rating) {
    const buttons = overlayElement.querySelectorAll('[data-rating]');
    buttons.forEach((btn, idx) => {
      const btnRating = parseInt(btn.dataset.rating);
      if (btnRating <= rating) {
        btn.style.background = 'white';
        btn.style.color = '#667eea';
      } else {
        btn.style.background = 'rgba(255,255,255,0.2)';
        btn.style.color = 'white';
      }
    });
  }
  
  // ────────────────────────────────────────────────
  // Обработчики действий
  // ────────────────────────────────────────────────
  async function handleAddToDatabase(info, filmData) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      // Если kp_id уже есть, добавляем сразу
      if (filmData?.kp_id) {
        try {
          const response = await apiRequest('POST', '/api/extension/add-film', {
            chat_id: data.linked_chat_id,
            user_id: data.linked_user_id,
            kp_id: filmData.kp_id,
            online_link: info.url
          });
          
          if (response.ok) {
            const result = await response.json();
            if (result.success) {
              // Обновляем данные и перерисовываем кнопки
              currentFilmData = { ...filmData, film_id: result.film_id, kp_id: filmData.kp_id };
              await renderButtons(info, currentFilmData);
            } else {
              alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
            }
          } else {
            alert('Ошибка сервера: ' + response.status);
          }
        } catch (fetchError) {
          console.error('[STREAMING] Ошибка fetch при добавлении в базу:', fetchError);
          alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
        }
      } else {
        // Нужно сначала найти через API
        alert('Поиск фильма... (это займет несколько секунд)');
        // Логика поиска будет в основной функции checkAndShowOverlay
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка добавления в базу:', e);
      alert('Ошибка добавления в базу: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  async function handleMarkEpisode(info, filmData, markAllPrevious) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      try {
        const response = await apiRequest('POST', '/api/extension/mark-episode', {
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          season: info.season,
          episode: info.episode,
          mark_all_previous: markAllPrevious,
          online_link: info.url
        });
        
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            alert('✅ Серия отмечена как просмотренная!');
            removeOverlay();
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          alert('Ошибка сервера: ' + response.status);
        }
      } catch (fetchError) {
        console.error('[STREAMING] Ошибка fetch при отметке серии:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка отметки серии:', e);
      alert('Ошибка отметки серии: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  async function handleMarkFilmWatched(info, filmData) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      try {
        const response = await apiRequest('POST', '/api/extension/mark-film-watched', {
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          online_link: info.url
        });
        
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            // Обновляем данные и показываем кнопки оценки
            currentFilmData = { ...filmData, watched: true };
            renderButtons(info, currentFilmData);
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          alert('Ошибка сервера: ' + response.status);
        }
      } catch (fetchError) {
        console.error('[STREAMING] Ошибка fetch при отметке фильма:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка отметки фильма:', e);
      alert('Ошибка отметки фильма: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  async function handleRating(info, filmData, rating) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      try {
        const response = await apiRequest('POST', '/api/extension/rate-film', {
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          rating: rating
        });
        
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            // Показываем сообщение об успехе
            const container = overlayElement.querySelector('#mpp-buttons-container');
            if (container) {
              container.innerHTML = `
                <div style="text-align: center; padding: 20px;">
                  <div style="font-size: 24px; margin-bottom: 8px;">✅</div>
                  <div style="font-weight: 600;">Оценка принята!</div>
                </div>
              `;
            }
            
            // Закрываем через 2 секунды
            setTimeout(() => {
              removeOverlay();
            }, 2000);
            
            // Если оценка высокая (≥7), отправляем рекомендации
            if (rating >= 7 && result.recommendations) {
              // Рекомендации уже отправлены в бота через API
            }
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          alert('Ошибка сервера: ' + response.status);
        }
      } catch (fetchError) {
        console.error('[STREAMING] Ошибка fetch при оценке:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка оценки:', e);
      alert('Ошибка оценки: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  // ────────────────────────────────────────────────
  // Основная логика проверки и показа плашки
  // ────────────────────────────────────────────────
  async function checkAndShowOverlay() {
    // Удаляем overlay при смене URL
    removeOverlay();
    
    const info = getContentInfo();
    console.log('[STREAMING] getContentInfo результат:', info);
    
    if (!info || !info.title) {
      console.log('[STREAMING] Пропуск: нет info или title');
      return;
    }
    
    // Для kinopoisk.ru - проверяем наличие видеоплеера
    if (hostname.includes('kinopoisk.ru') && !hostname.includes('hd.kinopoisk')) {
      // На kinopoisk.ru фильмы смотрятся на hd.kinopoisk.ru, здесь только описания
      // Проверяем наличие видеоплеера
      const hasVideoPlayer = document.querySelector('video, iframe[src*="player"], .player, [class*="player"]');
      if (!hasVideoPlayer) {
        console.log('[STREAMING] Пропуск: kinopoisk.ru без видеоплеера');
        return;
      }
    }
    
    // Для сериалов: показываем виджет ТОЛЬКО если определены сезон и серия
    // Для фильмов: показываем всегда (если есть title)
    if (info.isSeries && (!info.season || !info.episode)) {
      console.log('[STREAMING] Пропуск: сериал, но нет сезона/серии');
      return;
    }
    
    // Проверяем защиту от спама
    const shouldShow = shouldShowOverlay(info);
    console.log('[STREAMING] shouldShowOverlay результат:', shouldShow);
    
    if (!shouldShow) {
      console.log('[STREAMING] Пропуск: защита от спама');
      return;
    }
    
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id', 'has_notifications_access']);
      if (!data.linked_chat_id) {
        return; // Пользователь не привязан
      }
      
      // Проверяем подписку для функционала отметки серий (если еще не проверяли)
      if (data.has_notifications_access === undefined) {
        try {
          const subResponse = await apiRequest('GET', `/api/extension/check-subscription?chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`);
          if (subResponse.ok) {
            const subResult = await subResponse.json();
            if (subResult.success) {
              await chrome.storage.local.set({ has_notifications_access: subResult.has_notifications_access || false });
              data.has_notifications_access = subResult.has_notifications_access || false;
            }
          }
        } catch (subErr) {
          console.error('[STREAMING] Ошибка проверки подписки:', subErr);
          // Продолжаем работу, но без функционала отметки серий
          data.has_notifications_access = false;
        }
      }
      
      // Сначала проверяем локальный кэш
      let kpId = await findInLocalCache(info);
      let filmData = null;
      
      if (kpId) {
        // Нашли в кэше - получаем данные о фильме
        try {
          let url = `${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`;
          if (info.season && info.episode) {
            url += `&season=${info.season}&episode=${info.episode}`;
          }
          
          console.log('[STREAMING] Запрос film-info из кэша:', { kpId, url });
          
          const response = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
          
          console.log('[STREAMING] Ответ film-info из кэша:', { status: response.status, ok: response.ok });
          
          if (response.ok) {
            const result = await response.json();
            console.log('[STREAMING] Результат film-info из кэша:', result);
            if (result.success) {
              // ВАЖНО: film_id может быть 0 или null, проверяем явно
              const filmId = (result.film_id !== undefined && result.film_id !== null) ? result.film_id : null;
              filmData = {
                kp_id: kpId,
                film_id: filmId,
                watched: result.watched || false,
                rated: result.rated || false,
                has_unwatched_before: result.has_unwatched_before || false,
                current_episode_watched: result.current_episode_watched || false
              };
              console.log('[STREAMING] filmData после парсинга:', filmData, 'film_id из result:', result.film_id);
            } else {
              console.error('[STREAMING] API вернул success: false:', result);
            }
          } else {
            console.error('[STREAMING] HTTP ошибка:', response.status);
          }
        } catch (fetchError) {
          console.error('[STREAMING] Ошибка fetch film-info:', fetchError);
          // Если есть kp_id в кэше, но запрос упал - пробуем еще раз
          if (kpId) {
            console.log('[STREAMING] Повторная попытка запроса film-info для kp_id:', kpId);
            try {
              // Повторный запрос с таймаутом
              const retryResponse = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
              if (retryResponse.ok) {
                const retryResult = await retryResponse.json();
                if (retryResult.success) {
                  const filmId = (retryResult.film_id !== undefined && retryResult.film_id !== null) ? retryResult.film_id : null;
                  filmData = {
                    kp_id: kpId,
                    film_id: filmId,
                    watched: retryResult.watched || false,
                    rated: retryResult.rated || false,
                    has_unwatched_before: retryResult.has_unwatched_before || false,
                    current_episode_watched: retryResult.current_episode_watched || false
                  };
                  console.log('[STREAMING] Повторный запрос успешен, film_id:', filmId);
                } else {
                  throw new Error(retryResult.error || 'Unknown error');
                }
              } else {
                throw new Error(`HTTP ${retryResponse.status}`);
              }
            } catch (retryError) {
              console.error('[STREAMING] Повторный запрос тоже упал:', retryError);
              // Если повторный запрос тоже упал, показываем виджет с kp_id, но БЕЗ кнопки "Добавить в базу"
              // Предполагаем, что фильм может быть в базе, но мы не можем это проверить
              filmData = {
                kp_id: kpId,
                film_id: undefined,
                watched: false,
                rated: false,
                has_unwatched_before: false,
                current_episode_watched: false
              };
              console.log('[STREAMING] Продолжаем с kp_id, но film_id неизвестен:', kpId);
            }
          } else {
            // Нет kp_id - не показываем виджет
            console.log('[STREAMING] Пропуск: нет kp_id и ошибка film-info');
            return;
          }
        }
      } else {
        // Не нашли в кэше - ищем через API
        try {
          // Используем тот же формат, что и в боте: просто название, год передаем отдельно
          const searchKeyword = info.title.trim();
          const searchType = info.isSeries ? 'TV_SERIES' : 'FILM';
          const yearParam = info.year ? `&year=${info.year}` : '';
          const searchUrl = `${API_BASE_URL}/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(searchKeyword)}${yearParam}&type=${searchType}`;
          
          console.log('[STREAMING] Поиск фильма:', { searchKeyword, searchType, year: info.year, url: searchUrl });
          
          const searchResponse = await apiRequest('GET', `/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(searchKeyword)}${yearParam}&type=${searchType}`);
          
          console.log('[STREAMING] Ответ поиска:', { status: searchResponse.status, ok: searchResponse.ok });
          
          if (searchResponse.ok) {
            const searchResult = await searchResponse.json();
            console.log('[STREAMING] Результат поиска:', searchResult);
            if (searchResult.success && searchResult.kp_id) {
              kpId = searchResult.kp_id;
              
              // Сохраняем в кэш
              await saveToLocalCache(info, kpId);
              
              // Получаем данные о фильме
              try {
                let url = `${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`;
                if (info.season && info.episode) {
                  url += `&season=${info.season}&episode=${info.episode}`;
                }
                
                console.log('[STREAMING] Получение данных о фильме:', { kpId, url });
                
                const filmResponse = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
                
                console.log('[STREAMING] Ответ film-info:', { status: filmResponse.status, ok: filmResponse.ok });
                
                if (filmResponse.ok) {
                  const filmResult = await filmResponse.json();
                  console.log('[STREAMING] Результат film-info после поиска:', filmResult);
                  if (filmResult.success) {
                    // ВАЖНО: film_id может быть 0 или null, проверяем явно
                    const filmId = (filmResult.film_id !== undefined && filmResult.film_id !== null) ? filmResult.film_id : null;
                    filmData = {
                      kp_id: kpId,
                      film_id: filmId,
                      watched: filmResult.watched || false,
                      rated: filmResult.rated || false,
                      has_unwatched_before: filmResult.has_unwatched_before || false,
                      current_episode_watched: filmResult.current_episode_watched || false
                    };
                    console.log('[STREAMING] filmData после парсинга (после поиска):', filmData, 'film_id из result:', filmResult.film_id);
                  } else {
                    console.error('[STREAMING] API вернул success: false после поиска:', filmResult);
                    // Если фильм не найден в БД, но kp_id есть - создаем базовые данные
                    filmData = {
                      kp_id: kpId,
                      film_id: null,
                      watched: false,
                      rated: false,
                      has_unwatched_before: false,
                      current_episode_watched: false
                    };
                  }
                } else {
                  console.error('[STREAMING] HTTP ошибка после поиска:', filmResponse.status);
                  if (kpId) {
                    filmData = {
                      kp_id: kpId,
                      film_id: null,
                      watched: false,
                      rated: false,
                      has_unwatched_before: false,
                      current_episode_watched: false
                    };
                  }
                }
              } catch (filmFetchError) {
                console.error('[STREAMING] Ошибка fetch film-info после поиска:', filmFetchError);
                if (kpId) {
                  filmData = {
                    kp_id: kpId,
                    film_id: null,
                    watched: false,
                    rated: false,
                    has_unwatched_before: false,
                    current_episode_watched: false
                  };
                }
              }
            } else {
              console.log('[STREAMING] Фильм не найден в Kinopoisk API:', searchResult);
              // Фильм не найден - не показываем виджет
              return;
            }
          } else {
            console.error('[STREAMING] HTTP ошибка поиска:', searchResponse.status);
            // Если ошибка поиска - не показываем виджет
            return;
          }
        } catch (searchError) {
          console.error('[STREAMING] Ошибка fetch search-film-by-keyword:', searchError);
          // Если поиск не удался, пробуем еще раз с другим форматом (как в боте: "название год")
          if (info.title && info.year) {
            try {
              console.log('[STREAMING] Повторная попытка поиска с форматом "название год"');
              const retryKeyword = `${info.title} ${info.year}`.trim();
              const retryResponse = await apiRequest('GET', `/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(retryKeyword)}&type=${info.isSeries ? 'TV_SERIES' : 'FILM'}`);
              if (retryResponse.ok) {
                const retryResult = await retryResponse.json();
                if (retryResult.success && retryResult.kp_id) {
                  kpId = retryResult.kp_id;
                  console.log('[STREAMING] Повторный поиск успешен, kp_id:', kpId);
                  // Сохраняем в кэш
                  await saveToLocalCache(info, kpId);
                  // Теперь запрашиваем film-info
                  const filmResponse = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
                  if (filmResponse.ok) {
                    const filmResult = await filmResponse.json();
                    if (filmResult.success) {
                      const filmId = (filmResult.film_id !== undefined && filmResult.film_id !== null) ? filmResult.film_id : null;
                      filmData = {
                        kp_id: kpId,
                        film_id: filmId,
                        watched: filmResult.watched || false,
                        rated: filmResult.rated || false,
                        has_unwatched_before: filmResult.has_unwatched_before || false,
                        current_episode_watched: filmResult.current_episode_watched || false
                      };
                    }
                  }
                }
              }
            } catch (retryError) {
              console.error('[STREAMING] Повторный поиск тоже упал:', retryError);
            }
          }
          // Если все равно не нашли - не показываем виджет
          if (!kpId) {
            console.log('[STREAMING] Пропуск: фильм не найден, нет kp_id');
            return;
          }
        }
      }
      
      // Если не нашли фильм, создаем базовые данные
      if (!filmData) {
        // Если нет kp_id, значит фильм не найден - не показываем виджет
        if (!kpId) {
          console.log('[STREAMING] Пропуск: фильм не найден, нет kp_id');
          return;
        }
        filmData = {
          kp_id: kpId,
          film_id: null,
          watched: false,
          rated: false,
          has_unwatched_before: false,
          current_episode_watched: false
        };
      }
      
      // Показываем плашку (даже если были ошибки API, но kp_id есть)
      console.log('[STREAMING] Вызываем createOverlay с данными:', { info, filmData });
      await createOverlay(info, filmData);
      console.log('[STREAMING] createOverlay вызван');
      
    } catch (e) {
      console.error('[STREAMING] Ошибка проверки:', e);
      // Даже при ошибке показываем плашку с базовыми данными
      try {
        const filmData = {
          kp_id: null,
          film_id: null,
          watched: false,
          rated: false,
          has_unwatched_before: false,
          current_episode_watched: false
        };
        console.log('[STREAMING] Вызываем createOverlay с базовыми данными после ошибки:', { info, filmData });
        await createOverlay(info, filmData);
      } catch (overlayError) {
        console.error('[STREAMING] Ошибка создания плашки:', overlayError);
      }
    }
  }
  
  // ────────────────────────────────────────────────
  // Инициализация и наблюдение за изменениями
  // ────────────────────────────────────────────────
  function init() {
    // Первая проверка через 3 секунды после загрузки
    setTimeout(() => {
      checkAndShowOverlay();
    }, 3000);
    
      // Наблюдение за изменениями DOM (debounce 5 секунд для лучшей реакции на смену серий)
      const observer = new MutationObserver(() => {
        if (debounceTimer) {
          clearTimeout(debounceTimer);
        }
        debounceTimer = setTimeout(() => {
          // Проверяем, изменился ли сезон/серия
          const info = getContentInfo();
          if (info) {
            const currentHash = getContentHash(info);
            if (currentHash !== lastContentHash) {
              console.log('[STREAMING] MutationObserver: обнаружено изменение контента');
              lastContentHash = currentHash;
              const key = getContentKey(info);
              lastShown[key] = 0; // Сбрасываем кулдаун
              checkAndShowOverlay();
            }
          }
        }, 5000); // Уменьшили debounce до 5 секунд
      });
    
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      characterData: true
    });
    
    // Наблюдение за изменениями URL (для SPA)
    // Слушаем history.pushState и popstate для SPA навигации
    const originalPushState = history.pushState;
    const originalReplaceState = history.replaceState;
    
    history.pushState = function(...args) {
      originalPushState.apply(history, args);
      // Удаляем overlay при смене URL
      removeOverlay();
      setTimeout(() => {
        checkAndShowOverlay();
      }, 1000);
    };
    
    history.replaceState = function(...args) {
      originalReplaceState.apply(history, args);
      // Удаляем overlay при смене URL
      removeOverlay();
      setTimeout(() => {
        checkAndShowOverlay();
      }, 1000);
    };
    
    window.addEventListener('popstate', () => {
      // Удаляем overlay при смене URL
      removeOverlay();
      setTimeout(() => {
        checkAndShowOverlay();
      }, 1000);
    });
    
    // Дополнительная проверка URL через setInterval (fallback)
    let lastUrlCheck = location.href;
    setInterval(() => {
      if (location.href !== lastUrlCheck) {
        lastUrlCheck = location.href;
        setTimeout(() => {
          checkAndShowOverlay();
        }, 1000);
      }
    }, 2000);
    
    // Периодическая проверка для статичных URL (каждые 5 секунд для SPA)
    // Только если изменился контент (сезон/серия)
    checkInterval = setInterval(() => {
      const info = getContentInfo();
      if (info) {
        const currentHash = getContentHash(info);
        if (currentHash !== lastContentHash) {
          console.log('[STREAMING] Обнаружено изменение контента (hash изменился):', lastContentHash, '->', currentHash);
          const key = getContentKey(info);
          lastShown[key] = 0; // Сбрасываем кулдаун при смене сезона/серии
          checkAndShowOverlay();
          // lastContentHash обновляется только в shouldShowOverlay при показе; не трогаем здесь,
          // иначе checkAndShowOverlay видит «тот же контент» и пропускает показ.
        }
      }
    }, 5000);
  }
  
  // Запускаем при загрузке
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
  
})();

