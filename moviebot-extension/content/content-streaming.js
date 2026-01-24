// content-streaming.js
// Content script для стриминговых платформ: отслеживание просмотра фильмов и сериалов

(function() {
  'use strict';
  
  const API_BASE_URL = 'https://web-production-3921c.up.railway.app';
  
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
        selector: '#player-container div.VideoJS_titleWrapper__RPVJ7 > p.VideoJS_desc__kaIbK, p[class*="VideoJS_desc"]',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон[,\s.]*(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      }
    },
    
    'ivi.ru': {
      isSeries: () => {
        return document.querySelector('.postersListDesktop__seasonTitle') || 
               document.querySelector('.serieBadge') || 
               document.querySelector('#root .breadCrumbs__item:contains("Сериалы")');
      },
      title: {
        selector: 'title, meta[property="og:title"]',
        extract: (el) => {
          const text = el?.textContent || el?.content || '';
          return text.split(/[:|]/)[0]?.trim() || null;
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
        selector: '.PlayerData_episodeInfo__D7dT7',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
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
    
    // Если тот же контент, не показываем
    if (key === lastContentKey && hash === lastContentHash) {
      return false;
    }
    
    // Проверяем кулдаун (3 минуты)
    const now = Date.now();
    const last = lastShown[key] || 0;
    if (now - last < 3 * 60 * 1000) {
      return false;
    }
    
    lastShown[key] = now;
    lastContentKey = key;
    lastContentHash = hash;
    return true;
  }
  
  // ────────────────────────────────────────────────
  // Плашка с кнопками
  // ────────────────────────────────────────────────
  let overlayElement = null;
  let currentInfo = null;
  let currentKpId = null;
  let currentFilmId = null;
  let currentFilmData = null;
  
  function removeOverlay() {
    if (overlayElement) {
      overlayElement.remove();
      overlayElement = null;
    }
  }
  
  function createOverlay(info, filmData) {
    removeOverlay();
    
    currentInfo = info;
    currentFilmData = filmData;
    currentKpId = filmData?.kp_id || null;
    currentFilmId = filmData?.film_id || null;
    
    overlayElement = document.createElement('div');
    overlayElement.id = 'movieplanner-streaming-overlay';
    // Позиционируем плашку так, чтобы она не перекрывала видеопроигрыватель
    // Обычно видеопроигрыватель в центре/сверху, поэтому плашка справа внизу
    overlayElement.style.cssText = `
      position: fixed;
      bottom: 20px;
      right: 20px;
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
    `;
    
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
    
    // Кнопка закрытия
    overlayElement.querySelector('#mpp-close').addEventListener('click', () => {
      removeOverlay();
    });
    
    // Рендерим кнопки
    renderButtons(info, filmData);
  }
  
  function renderButtons(info, filmData) {
    const container = overlayElement.querySelector('#mpp-buttons-container');
    if (!container) return;
    
    container.innerHTML = '';
    
    const isInDatabase = filmData && filmData.film_id;
    
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
      // Фильм/сериал в базе
      if (info.isSeries) {
        // Сериал
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
        
        // Проверяем, есть ли непросмотренные серии до текущей
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
      } else {
        // Фильм
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
        const response = await fetch(`${API_BASE_URL}/api/extension/add-film`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: data.linked_chat_id,
            user_id: data.linked_user_id,
            kp_id: filmData.kp_id,
            online_link: info.url
          })
        });
        
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            // Обновляем данные и перерисовываем кнопки
            currentFilmData = { ...filmData, film_id: result.film_id, kp_id: filmData.kp_id };
            renderButtons(info, currentFilmData);
          }
        }
      } else {
        // Нужно сначала найти через API
        alert('Поиск фильма... (это займет несколько секунд)');
        // Логика поиска будет в основной функции checkAndShowOverlay
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка добавления в базу:', e);
      alert('Ошибка добавления в базу');
    }
  }
  
  async function handleMarkEpisode(info, filmData, markAllPrevious) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      const response = await fetch(`${API_BASE_URL}/api/extension/mark-episode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          season: info.season,
          episode: info.episode,
          mark_all_previous: markAllPrevious,
          online_link: info.url
        })
      });
      
      if (response.ok) {
        const result = await response.json();
        if (result.success) {
          alert('✅ Серия отмечена как просмотренная!');
          removeOverlay();
        } else {
          alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
        }
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка отметки серии:', e);
      alert('Ошибка отметки серии');
    }
  }
  
  async function handleMarkFilmWatched(info, filmData) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      const response = await fetch(`${API_BASE_URL}/api/extension/mark-film-watched`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          online_link: info.url
        })
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
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка отметки фильма:', e);
      alert('Ошибка отметки фильма');
    }
  }
  
  async function handleRating(info, filmData, rating) {
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      
      const response = await fetch(`${API_BASE_URL}/api/extension/rate-film`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: data.linked_chat_id,
          user_id: data.linked_user_id,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id,
          rating: rating
        })
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
      }
    } catch (e) {
      console.error('[STREAMING] Ошибка оценки:', e);
      alert('Ошибка оценки');
    }
  }
  
  // ────────────────────────────────────────────────
  // Основная логика проверки и показа плашки
  // ────────────────────────────────────────────────
  async function checkAndShowOverlay() {
    const info = getContentInfo();
    if (!info || !info.title) {
      return;
    }
    
    // Проверяем защиту от спама
    if (!shouldShowOverlay(info)) {
      return;
    }
    
    try {
      const data = await chrome.storage.local.get(['linked_chat_id', 'linked_user_id']);
      if (!data.linked_chat_id) {
        return; // Пользователь не привязан
      }
      
      // Сначала проверяем локальный кэш
      let kpId = await findInLocalCache(info);
      let filmData = null;
      
      if (kpId) {
        // Нашли в кэше - получаем данные о фильме
        const url = `${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`;
        if (info.season && info.episode) {
          url += `&season=${info.season}&episode=${info.episode}`;
        }
        const response = await fetch(url);
        if (response.ok) {
          const result = await response.json();
          if (result.success) {
            filmData = {
              kp_id: kpId,
              film_id: result.film_id || null,
              watched: result.watched || false,
              rated: result.rated || false,
              has_unwatched_before: result.has_unwatched_before || false
            };
          }
        }
      } else {
        // Не нашли в кэше - ищем через API
        const searchKeyword = `${info.title} ${info.year || ''}`.trim();
        const searchType = info.isSeries ? 'TV_SERIES' : 'FILM';
        
        const searchResponse = await fetch(`${API_BASE_URL}/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(searchKeyword)}&year=${info.year || ''}&type=${searchType}`);
        if (searchResponse.ok) {
          const searchResult = await searchResponse.json();
          if (searchResult.success && searchResult.kp_id) {
            kpId = searchResult.kp_id;
            
            // Сохраняем в кэш
            await saveToLocalCache(info, kpId);
            
            // Получаем данные о фильме
            let url = `${API_BASE_URL}/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`;
            if (info.season && info.episode) {
              url += `&season=${info.season}&episode=${info.episode}`;
            }
            const filmResponse = await fetch(url);
            if (filmResponse.ok) {
              const filmResult = await filmResponse.json();
              if (filmResult.success) {
                filmData = {
                  kp_id: kpId,
                  film_id: filmResult.film_id || null,
                  watched: filmResult.watched || false,
                  rated: filmResult.rated || false,
                  has_unwatched_before: filmResult.has_unwatched_before || false
                };
              }
            }
          }
        }
      }
      
      // Если не нашли фильм, создаем базовые данные
      if (!filmData) {
        filmData = {
          kp_id: kpId || null,
          film_id: null,
          watched: false,
          rated: false,
          has_unwatched_before: false
        };
      }
      
      // Показываем плашку
      createOverlay(info, filmData);
      
    } catch (e) {
      console.error('[STREAMING] Ошибка проверки:', e);
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
    
    // Наблюдение за изменениями DOM (debounce 15 секунд)
    const observer = new MutationObserver(() => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
      debounceTimer = setTimeout(() => {
        checkAndShowOverlay();
      }, 15000);
    });
    
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      characterData: true
    });
    
    // Наблюдение за изменениями URL (для SPA)
    let lastUrlCheck = location.href;
    setInterval(() => {
      if (location.href !== lastUrlCheck) {
        lastUrlCheck = location.href;
        setTimeout(() => {
          checkAndShowOverlay();
        }, 1000);
      }
    }, 500);
    
    // Периодическая проверка для статичных URL (каждые 30 секунд)
    checkInterval = setInterval(() => {
      const currentHash = getContentHash(getContentInfo() || {});
      if (currentHash !== lastContentHash) {
        lastContentHash = currentHash;
        checkAndShowOverlay();
      }
    }, 30000);
  }
  
  // Запускаем при загрузке
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
  
})();

