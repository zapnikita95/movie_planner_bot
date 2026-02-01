// content-streaming.js
// Content script для стриминговых платформ: отслеживание просмотра фильмов и сериалов

(function() {
  'use strict';
  
  const API_BASE_URL = 'https://web-production-3921c.up.railway.app';
  
  // ────────────────────────────────────────────────
  // Вспомогательная функция для API запросов через background script
  // ────────────────────────────────────────────────
  function isContextInvalidated(e) {
    const msg = (e && e.message) ? String(e.message) : '';
    return /Extension context invalidated/i.test(msg) || /Message closed/i.test(msg);
  }

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
        try {
          chrome.runtime.sendMessage(message, (response) => {
            try {
              if (chrome.runtime.lastError) {
                const err = new Error(chrome.runtime.lastError.message);
                reject(err);
                return;
              }
              if (!response) {
                reject(new Error('No response from background script'));
                return;
              }
              if (!response.success) {
                reject(new Error(response.error || 'Unknown error'));
                return;
              }
              resolve({
                ok: response.status >= 200 && response.status < 300,
                status: response.status,
                json: async () => response.data
              });
            } catch (cbErr) {
              reject(cbErr);
            }
          });
        } catch (sendErr) {
          reject(sendErr);
        }
      });
    } catch (error) {
      if (isContextInvalidated(error)) throw new Error('Extension context invalidated');
      throw error;
    }
  }
  
  // Поддерживаемые сайты
  const supportedHosts = [
    'tvoe.live', 'ivi.ru', 'okko.tv', 'kinopoisk.ru', 'hd.kinopoisk.ru',
    'premier.one', 'wink.ru', 'start.ru', 'amediateka.ru', 'kion.ru',
    'rezka.ag', 'rezka.ad', 'hdrezka', 'lordfilm', 'allserial', 'boxserial',
    'kino.pub', 'smotreshka.tv'
  ];
  
  const hostname = window.location.hostname.toLowerCase();
  if (!supportedHosts.some(h => hostname.includes(h))) {
    return; // Сайт не поддерживается
  }

  function storageLocal() {
    try {
      return (typeof chrome !== 'undefined' && chrome?.storage?.local) ? chrome.storage.local : null;
    } catch (e) { return null; }
  }
  
  // ────────────────────────────────────────────────
  // Конфигурации парсинга для каждого сайта
  // ────────────────────────────────────────────────
  const siteConfigs = {
    'tvoe.live': {
      isSeries: () => {
        const btn = document.querySelector('#headNav > div > button:nth-child(2) > div');
        const t = btn?.textContent?.trim() || '';
        if (/О сериале/i.test(t)) return true;
        if (/О фильме/i.test(t)) return false;
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
        selector: '#root .headerBar .breadCrumbs__item:first-child a span[itemprop="name"], title, meta[property="og:title"]',
        extract: (el) => {
          // Приоритет: breadcrumb с itemprop="name"
          if (el?.getAttribute?.('itemprop') === 'name') {
            const text = el?.textContent?.trim() || '';
            // Обрезаем до скобки если есть
            const beforeParen = text.split(/\s*\(/)[0]?.trim();
            return beforeParen || text || null;
          }
          // Fallback: title или og:title
          const text = el?.textContent || el?.content || '';
          // Обрезаем до скобки если есть
          let cleanTitle = text.split(/\s*\(/)[0]?.trim() || '';
          cleanTitle = cleanTitle.split(/[:|]/)[0]?.trim() || '';
          cleanTitle = cleanTitle.replace(/^Сериал\s+/i, '');
          cleanTitle = cleanTitle.split(/\s+смотреть/i)[0]?.trim() || cleanTitle;
          cleanTitle = cleanTitle.split(/\s+в хорошем/i)[0]?.trim() || cleanTitle;
          cleanTitle = cleanTitle.replace(/\s+\d+\s*сезон\s*\d+\s*серия/i, '').trim();
          cleanTitle = cleanTitle.replace(/\s+\d+\s*сезон/i, '').trim();
          cleanTitle = cleanTitle.replace(/\s+\d+\s*серия/i, '').trim();
          cleanTitle = cleanTitle.replace(/\s+\d{4}\s*$/, '').trim();
          return cleanTitle || null;
        }
      },
      searchBaseTitle: (title) => {
        if (!title) return null;
        let base = title.replace(/\s*—\s*[^(]+(\s*\([^)]*\))?\s*$/i, '').trim();
        base = base.replace(/\s*\([^)]*[Чч]асть\s*\d+[^)]*\)\s*$/i, '').trim();
        base = base.replace(/\s*\([^)]*[Сс]езон\s*\d+[^)]*\)\s*$/i, '').trim();
        return base || title;
      },
      year: {
        selector: '.paramsList__container a[href*="/movies/"], .paramsList__container a[href*="/series/"]',
        extract: (el) => el?.textContent?.trim() || null
      },
      seasonEpisode: {
        selector: '#root .headerBar .breadCrumbs__item:nth-child(2) a span[itemprop="name"], #root .headerBar .breadCrumbs__item:nth-child(3) div span[itemprop="name"], .postersListDesktop__listTitle span, .serieBadge button div, .nbl-button__primaryText, .meta__serieBadge button div',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          // Формат "Серия 3 сезон 3" - сначала серия, потом сезон (обратный порядок!)
          const mReverse = t.match(/Серия\s+(\d+)\s+сезон\s+(\d+)/i);
          if (mReverse) {
            return { season: parseInt(mReverse[2]), episode: parseInt(mReverse[1]) };
          }
          // Обычный формат "3 сезон, 3 серия" или "сезон 3, серия 3"
          const m = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i) || t.match(/сезон\s*(\d+).*?серия\s*(\d+)/i);
          if (m) {
            return { season: parseInt(m[1]), episode: parseInt(m[2]) };
          }
          // Если это отдельный элемент с сезоном или серией
          if (t.match(/^\d+\s*сезон$/i)) {
            const seasonNum = parseInt(t.match(/(\d+)/)?.[1]);
            // Ищем серию в соседнем элементе
            const episodeEl = document.querySelector('#root .headerBar .breadCrumbs__item:nth-child(3) div span[itemprop="name"]');
            if (episodeEl) {
              const episodeText = episodeEl.textContent?.trim() || '';
              const episodeMatch = episodeText.match(/(\d+)\s*серия/i);
              if (episodeMatch && seasonNum) {
                return { season: seasonNum, episode: parseInt(episodeMatch[1]) };
              }
            }
            return seasonNum ? { season: seasonNum, episode: null } : null;
          }
          if (t.match(/^\d+\s*серия$/i)) {
            const episodeNum = parseInt(t.match(/(\d+)/)?.[1]);
            // Ищем сезон в соседнем элементе
            const seasonEl = document.querySelector('#root .headerBar .breadCrumbs__item:nth-child(2) a span[itemprop="name"]');
            if (seasonEl) {
              const seasonText = seasonEl.textContent?.trim() || '';
              const seasonMatch = seasonText.match(/(\d+)\s*сезон/i);
              if (seasonMatch && episodeNum) {
                return { season: parseInt(seasonMatch[1]), episode: episodeNum };
              }
            }
            return episodeNum ? { season: null, episode: episodeNum } : null;
          }
          return null;
        }
      }
    },
    
    'okko.tv': {
      isSeries: () => {
        const path = window.location.pathname || '';
        if (path.includes('/serial/')) return true;
        const meta = document.querySelector('meta[property="og:title"]');
        const c = meta?.content || '';
        if (/сезон|серии/i.test(c)) return true;
        const title = document.querySelector('title');
        return !!(title?.textContent?.includes('сезон') || title?.textContent?.includes('серии'));
      },
      title: {
        selector: 'meta[property="og:title"], title',
        extract: (el) => {
          const c = (el?.content || el?.textContent || '').trim();
          // Берём название до первой скобки: "Бык (фильм, 2019) ..." -> "Бык"
          const beforeParen = c.split(/\s*\(/)[0]?.trim();
          if (beforeParen) return beforeParen;
          // Если скобок нет, пробуем по сезону
          const beforeSeason = c.split(/\s+[Сс]езон\s*\d/i)[0]?.trim();
          return beforeSeason || c.split(/[\(\[]/)[0]?.trim() || null;
        }
      },
      year: {
        selector: 'span[test-id="meta_release_date"], title, meta[property="og:title"]',
        extract: (el) => {
          // Сначала пробуем из span с датой
          const raw = el?.textContent || el?.content || '';
          // Из title/og:title: "Бык (фильм, 2019)" -> "2019"
          const yearMatch = raw.match(/\((?:фильм|сериал)[,\s]+(\d{4})/i);
          if (yearMatch) return yearMatch[1];
          // Из span: "2019" или "2019-2020"
          const y = raw.trim().split('-')[0]?.trim();
          return /^\d{4}$/.test(y) ? y : (raw.match(/\d{4}/)?.[0] || null);
        }
      },
      seasonEpisode: {
        selector: '[test-id="player_content_title"], h4[test-id="content_progress_title"], span.RQ6wn_Q0, img[alt*="Сезон"]',
        extract: (el) => {
          const t = (el?.textContent || el?.alt || '').trim();
          let m = t.match(/(\d+)\s*сезон[.\s]*(\d+)\s*серия/i) || t.match(/сезон\s*(\d+)[.\s]*серия\s*(\d+)/i);
          if (m) return { season: parseInt(m[1]), episode: parseInt(m[2]) };
          m = t.match(/Сезон\s*(\d+)[.\s]*Серия\s*(\d+)/i);
          return m ? { season: parseInt(m[1]), episode: parseInt(m[2]) } : null;
        }
      },
      // Проверка страницы: только /movie/... и /serial/...
      isValidPage: () => {
        const path = window.location.pathname || '';
        return /^\/(movie|serial)\/[^/]+/.test(path);
      }
    },
    
    'kinopoisk.ru,hd.kinopoisk.ru': {
      isSeries: () => {
        // 1. Проверяем URL параметры - если есть season или episode, это сериал
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.has('season') || urlParams.has('episode')) {
          return true;
        }
        // 2. Проверяем title на слово "сериал"
        const titleEl = document.querySelector('title[data-tid="HdSeoHead"], title');
        const t = titleEl?.textContent || '';
        if (/\(сериал\b/i.test(t) || /\bсериал\b/i.test(t) || /все серии/i.test(t)) {
          return true;
        }
        // 3. Проверяем URL path на /series/
        if (/\/series\//i.test(window.location.pathname)) {
          return true;
        }
        return false;
      },
      title: {
        selector: 'title[data-tid="HdSeoHead"], title',
        extract: (el) => {
          const text = el?.textContent || '';
          // "Ассасины. Начало (сериал, все серии), 2024 — смотреть..." → "Ассасины. Начало"
          // Берём всё до первой скобки "("
          const beforeParen = text.split(/\s*\(/)[0]?.trim();
          if (beforeParen && beforeParen.length > 0 && !beforeParen.includes('Кинопоиск')) {
            return beforeParen;
          }
          // Fallback: до запятой или тире
          const fallback = text.split(/[,—]/)[0]?.trim();
          if (fallback && !fallback.includes('Кинопоиск')) {
            return fallback;
          }
          return null;
        }
      },
      year: {
        selector: 'title[data-tid="HdSeoHead"], title',
        extract: (el) => {
          const text = el?.textContent || '';
          // "Ассасины. Начало (сериал, все серии), 2024 — смотреть..." → "2024"
          // Ищем год после скобок: ), 2024 или просто отдельно стоящий год
          const m = text.match(/\)\s*,?\s*(\d{4})/);
          if (m) return m[1];
          // Fallback: ищем год в формате ", 2024" или "2024 —"
          const m2 = text.match(/,\s*(\d{4})\s*[—-]/) || text.match(/(\d{4})\s*[—-]/);
          if (m2) return m2[1];
          return null;
        }
      },
      seasonEpisode: {
        getSeasonEpisode: () => {
          // 1. Сначала пробуем из URL параметров
          const urlParams = new URLSearchParams(window.location.search);
          const seasonFromUrl = urlParams.get('season');
          const episodeFromUrl = urlParams.get('episode');
          if (seasonFromUrl && episodeFromUrl) {
            return { season: parseInt(seasonFromUrl), episode: parseInt(episodeFromUrl) };
          }
          // 2. Пробуем из DOM элемента ".styles_subtitle__PPaVH"
          const subtitleEl = document.querySelector('.styles_subtitle__PPaVH');
          if (subtitleEl) {
            const t = subtitleEl.textContent?.trim() || '';
            // "1 сезон, 1 серия. Привет фром Марс" → {season: 1, episode: 1}
            const m = t.match(/(\d+)\s*сезон[.\s,]*(\d+)\s*серия/i);
            if (m) {
              return { season: parseInt(m[1]), episode: parseInt(m[2]) };
            }
          }
          // 3. Fallback - другие селекторы
          const selectors = ['.styles_extraInfo__A3zOn div', '[data-tid="ContentInfoItem"]', '.ContentInfoItem_root__J1fBw span'];
          for (const sel of selectors) {
            const el = document.querySelector(sel);
            if (el) {
              const t = el.textContent?.trim() || '';
              const m = t.match(/(\d+)\s*сезон[.\s,]*(\d+)\s*серия/i);
              if (m) {
                return { season: parseInt(m[1]), episode: parseInt(m[2]) };
              }
            }
          }
          return null;
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
      },
      // Проверка страницы: исключаем главную страницу
      isValidPage: () => {
        const path = window.location.pathname || '';
        // Главная страница - только "/"
        return path !== '/' && path !== '';
      }
    },
    
    'wink.ru': {
      isSeries: () => {
        const meta = document.querySelector('meta[property="og:title"]');
        const t = meta?.content || document.querySelector('title')?.textContent || '';
        return /сериал/i.test(t) || false;
      },
      title: {
        selector: '#root .r15qqrn5 main .t8imw3x .t1dmv9mm div .n18nc5fw span[data-test="player-content-name"], meta[property="og:title"]',
        extract: (el) => {
          // Приоритет: span с data-test="player-content-name"
          if (el?.getAttribute?.('data-test') === 'player-content-name') {
            const text = el?.textContent?.trim() || '';
            // Обрезаем до скобки если есть
            const beforeParen = text.split(/\s*\(/)[0]?.trim();
            return beforeParen || text || null;
          }
          // Fallback: meta[property="og:title"]
          const text = (el?.content || el?.getAttribute?.('content') || '').trim();
          // Обрезаем до скобки - берем только название до "("
          const beforeParen = text.split(/\s*\(/)[0]?.trim();
          if (beforeParen) {
            // Убираем префиксы типа "Плеер сериал" или "Плеер фильм"
            let clean = beforeParen.replace(/^Плеер\s+(?:сериал|фильм|мультсериал)\s+/i, '').trim();
            // Убираем "смотреть фильм онлайн" и подобное
            clean = clean.replace(/\s+смотреть\s+(?:фильм|сериал|мультсериал)\s+онлайн.*$/i, '').trim();
            return clean || null;
          }
          // Старый fallback для совместимости
          const mSeries = text.match(/Плеер\s+(?:сериал|фильм)\s+(.+?)\s+серия\s+\d+/i);
          if (mSeries) return mSeries[1].trim();
          const mFilm = text.match(/Плеер\s+фильм\s+(.+?)\s*\((\d{4})\)/i);
          if (mFilm) return mFilm[1].trim();
          return text.replace(/Плеер\s+(?:сериал|фильм)\s+/i, '').split(/\s+серия\s+\d+/i)[0]?.trim()
            || text.split(/[,(（]/)[0]?.replace(/Плеер\s+(?:сериал|фильм)\s+/i, '').trim() || null;
        }
      },
      year: {
        selector: 'meta[property="og:title"]',
        extract: (el) => {
          const text = el?.content || el?.getAttribute?.('content') || '';
          // Ищем год в скобках после названия: "Название (2025) смотреть..."
          const m = text.match(/\((\d{4})\)/);
          if (m) return m[1];
          // Fallback: ищем любой 4-значный год
          const m2 = text.match(/(\d{4})/);
          return m2 ? m2[1] : null;
        }
      },
      searchBaseTitle: (title) => {
        return (title || '').replace(/\s+серия\s+\d+$/i, '').trim() || title;
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
        selector: '#player-popup-data .PlayerData_title__SRmNd, title[data-next-head], title',
        extract: (el) => {
          // Приоритет: PlayerData_title__SRmNd из popup плеера
          if (el?.classList?.contains?.('PlayerData_title__SRmNd')) {
            const text = el?.textContent?.trim() || '';
            return text || null;
          }
          // Fallback: title
          const text = el?.textContent || '';
          // "Сериал Побег (2026) смотреть онлайн..." → "Побег"
          let title = text.replace(/^(Сериал|Фильм)\s+/, '').split(/смотреть/)[0]?.trim() || '';
          // Убираем год в скобках: "Побег (2026)" → "Побег"
          title = title.replace(/\s*\(\d{4}\)\s*$/, '').trim();
          return title || null;
        }
      },
      year: {
        selector: 'title',
        extract: (el) => el?.textContent?.match(/\((\d{4})\)/)?.[1] || el?.textContent?.match(/(\d{4})/)?.[1]
      },
      seasonEpisode: {
        selector: '#player-popup-data .PlayerData_episodeInfo__D7dT7, .PlayButton_playButtonContext__4XH_C, .PlayerData_episodeInfo__D7dT7',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          // Формат: "1 сезон, 1 серия" или "1 сезон,  1 серия" (с двойным пробелом)
          const m = t.match(/(\d+)\s*сезон\s*,?\s*(\d+)\s*серия/i);
          if (m) {
            return { season: parseInt(m[1]), episode: parseInt(m[2]) };
          }
          return null;
        }
      },
      // Флаг для отслеживания клика на play на главной
      playButtonClicked: false
    },
    
    'rezka,hdrezka': {
      isSeries: () => {
        const h1 = document.querySelector('h1.full-article__title');
        if (!h1) return false;
        const txt = h1.textContent || '';
        if (/сезон|серия/i.test(txt)) return true;
        const seasonSpan = h1.querySelector('.season');
        return !!(seasonSpan && /сезон|серия/i.test(seasonSpan.textContent || ''));
      },
      title: {
        selector: 'h1.full-article__title',
        extract: (el) => {
          const text = (el?.textContent || '').replace(/\s+/g, ' ').trim();
          const before = text.split(/\d{4}|сезон|серия/i)[0]?.trim() || '';
          return before || null;
        }
      },
      year: {
        selector: 'h1.full-article__title span',
        extract: (el) => {
          const m = (el?.textContent || '').match(/\d{4}/);
          return m ? m[0] : null;
        }
      },
      seasonEpisode: {
        getSeasonEpisode: () => {
          const parseNum = (t, kind) => {
            if (!t || typeof t !== 'string') return null;
            const s = String(t).trim();
            const m = kind === 'season'
              ? (s.match(/(\d+)\s*сезон/i) || s.match(/Сезон\s*(\d+)/i))
              : (s.match(/(\d+)\s*серия/i) || s.match(/Серия\s*(\d+)/i));
            return m ? parseInt(m[1]) : null;
          };
          let season = null, episode = null;
          const cplayS = document.querySelector('#player .list_5Wf > div:nth-child(1) .headText_3i3');
          const cplayE = document.querySelector('#player .list_5Wf > div:nth-child(2) .headText_3i3');
          if (cplayS) season = parseNum(cplayS.textContent, 'season');
          if (cplayE) episode = parseNum(cplayE.textContent, 'episode');
          const aplayS = document.querySelector('#allplay .selects.ui > div:nth-child(1) .select__item-text');
          const aplayE = document.querySelector('#allplay .selects.ui > div:nth-child(2) .select__item-text');
          if (aplayS && season == null) season = parseNum(aplayS.textContent, 'season');
          if (aplayE && episode == null) episode = parseNum(aplayE.textContent, 'episode');
          if (season != null || episode != null) {
            return { season: season ?? null, episode: episode ?? null };
          }
          return null;
        }
      }
    },
    
    'lordfilm,lordserial': {
      isSeries: () => {
        // Проверяем хлебные крошки: если второй элемент = "Сериалы", то это сериал
        const breadcrumb = document.querySelector('#dle-speedbar');
        if (!breadcrumb) return false;
        const items = breadcrumb.querySelectorAll('span[itemprop="name"]');
        // items[0] = "LordFilm", items[1] = "Фильмы" или "Сериалы"
        if (items.length >= 2) {
          const category = items[1]?.textContent?.trim() || '';
          return category === 'Сериалы';
        }
        return breadcrumb.textContent?.includes('Сериалы') || false;
      },
      title: {
        selector: '#dle-speedbar',
        extract: (el) => {
          // Хлебные крошки: "LordFilm » Фильмы » Гарри Поттер и Тайная комната (2002)"
          // Берём всё после последнего "»"
          const text = (el?.textContent || '').trim();
          const parts = text.split('»');
          if (parts.length >= 3) {
            const lastPart = parts[parts.length - 1].trim();
            // Убираем год в скобках: "Гарри Поттер и Тайная комната (2002)" -> "Гарри Поттер и Тайная комната"
            return lastPart.replace(/\s*\(\d{4}\)\s*$/, '').trim() || null;
          }
          return null;
        }
      },
      year: {
        selector: '#dle-speedbar',
        extract: (el) => {
          // Хлебные крошки: "LordFilm » Фильмы » Гарри Поттер и Тайная комната (2002)"
          // Берём год из последней части
          const text = (el?.textContent || '').trim();
          const parts = text.split('»');
          if (parts.length >= 3) {
            const lastPart = parts[parts.length - 1].trim();
            const yearMatch = lastPart.match(/\((\d{4})\)/);
            return yearMatch ? yearMatch[1] : null;
          }
          return null;
        }
      },
      seasonEpisode: {
        getSeasonEpisode: () => {
          const parseNum = (t, kind) => {
            if (!t || typeof t !== 'string') return null;
            const s = String(t).trim();
            if (kind === 'season') {
              const m = s.match(/Сезон\s*(\d+)/i) || s.match(/(\d+)\s*сезон/i);
              return m ? parseInt(m[1]) : null;
            }
            const m = s.match(/(\d+)\s*серия/i) || s.match(/Серия\s*(\d+)/i) || s.match(/Эпизод\s*(\d+)/i);
            return m ? parseInt(m[1], 10) : null;
          };
          let season = null, episode = null;
          const cplayS = document.querySelector('#player .list_5Wf > div:nth-child(1) .headText_3i3');
          const cplayE = document.querySelector('#player .list_5Wf > div:nth-child(2) .headText_3i3');
          if (cplayS) season = parseNum(cplayS.textContent, 'season');
          if (cplayE) episode = parseNum(cplayE.textContent, 'episode');
          const aplayS = document.querySelector('#allplay .selects.ui > div:nth-child(1) .select__item-text');
          const aplayE = document.querySelector('#allplay .selects.ui > div:nth-child(2) .select__item-text');
          if (aplayS && season == null) season = parseNum(aplayS.textContent, 'season');
          if (aplayE && episode == null) episode = parseNum(aplayE.textContent, 'episode');
          const ctrlS = document.querySelector('#controls-root > div > div:nth-child(1) > div > div');
          const ctrlE = document.querySelector('#controls-root > div > div:nth-child(2) > div > div');
          if (ctrlS && season == null) season = parseNum(ctrlS.textContent, 'season');
          if (ctrlE && episode == null) episode = parseNum(ctrlE.textContent, 'episode');
          const items = document.querySelectorAll('.item-el.item-st');
          items.forEach((el) => {
            const t = (el?.textContent || '').trim();
            if (/сезон/i.test(t)) { if (season == null) season = parseNum(t, 'season'); }
            else if (/серия|эпизод/i.test(t)) { if (episode == null) episode = parseNum(t, 'episode'); }
          });
          if (season != null || episode != null) {
            return { season: season ?? null, episode: episode ?? null };
          }
          return null;
        }
      }
    },
    
    'allserial': {
      isSeries: () => true,
      title: {
        selector: 'h1.short-title',
        extract: (el) => (el?.textContent || '').split(/\d+\s*сезон/)[0]?.trim() || null
      },
      year: {
        selector: 'main article ul li span[itemprop="datePublished"], span[itemprop="datePublished"]',
        extract: (el) => (el?.textContent || '').trim().replace(/\D/g, '').slice(0, 4) || null
      },
      seasonEpisode: {
        getSeasonEpisode: () => {
          const parseNum = (t, kind) => {
            if (!t || typeof t !== 'string') return null;
            const s = String(t).trim();
            const m = kind === 'season'
              ? (s.match(/(\d+)\s*сезон/i) || s.match(/Сезон\s*(\d+)/i))
              : (s.match(/(\d+)\s*серия/i) || s.match(/Серия\s*(\d+)/i));
            return m ? parseInt(m[1], 10) : null;
          };
          let season = null, episode = null;
          const fs = document.querySelector('#filterS-styler .jq-selectbox__select-text span');
          const fe = document.querySelector('#filterE-styler .jq-selectbox__select-text span');
          if (fs) season = parseNum(fs.textContent, 'season');
          if (fe) episode = parseNum(fe.textContent, 'episode');
          const cplayS = document.querySelector('#player .list_5Wf > div:nth-child(1) .headText_3i3');
          const cplayE = document.querySelector('#player .list_5Wf > div:nth-child(2) .headText_3i3');
          if (cplayS && season == null) season = parseNum(cplayS.textContent, 'season');
          if (cplayE && episode == null) episode = parseNum(cplayE.textContent, 'episode');
          const aplayS = document.querySelector('#allplay .selects.ui > div:nth-child(1) .select__item-text');
          const aplayE = document.querySelector('#allplay .selects.ui > div:nth-child(2) .select__item-text');
          if (aplayS && season == null) season = parseNum(aplayS.textContent, 'season');
          if (aplayE && episode == null) episode = parseNum(aplayE.textContent, 'episode');
          if (season != null || episode != null) {
            return { season: season ?? null, episode: episode ?? null };
          }
          return null;
        }
      }
    },
    
    'boxserial': {
      isSeries: () => true,
      title: {
        selector: '.page__titles h1, article .page__header h1',
        extract: (el) => (el?.textContent || '').split(/[1,2,3]|сезон/i)[0]?.replace(/\s+смотреть.*$/i, '').trim() || null
      },
      year: {
        selector: '.page__info ul li:nth-child(1), ul.page__info li:first-child',
        extract: (el) => {
          const m = (el?.textContent || '').trim().match(/\d{4}/);
          return m ? m[0] : null;
        }
      },
      seasonEpisode: {
        getSeasonEpisode: () => {
          const parseNum = (t, kind) => {
            if (!t || typeof t !== 'string') return null;
            const s = String(t).trim();
            if (kind === 'season') {
              const m = s.match(/Сезон\s*(\d+)/i) || s.match(/(\d+)\s*сезон/i);
              return m ? parseInt(m[1], 10) : null;
            }
            const m = s.match(/(\d+)\s*серия/i) || s.match(/Серия\s*(\d+)/i) || s.match(/Эпизод\s*(\d+)/i);
            return m ? parseInt(m[1], 10) : null;
          };
          let season = null, episode = null;
          const ctrlS = document.querySelector('#controls-root > div > div:nth-child(1) > div > div');
          const ctrlE = document.querySelector('#controls-root > div > div:nth-child(2) > div > div');
          if (ctrlS) season = parseNum(ctrlS.textContent, 'season');
          if (ctrlE) episode = parseNum(ctrlE.textContent, 'episode');
          const cplayS = document.querySelector('#player .list_5Wf > div:nth-child(1) .headText_3i3');
          const cplayE = document.querySelector('#player .list_5Wf > div:nth-child(2) .headText_3i3');
          if (cplayS && season == null) season = parseNum(cplayS.textContent, 'season');
          if (cplayE && episode == null) episode = parseNum(cplayE.textContent, 'episode');
          const aplayS = document.querySelector('#allplay .selects.ui > div:nth-child(1) .select__item-text');
          const aplayE = document.querySelector('#allplay .selects.ui > div:nth-child(2) .select__item-text');
          if (aplayS && season == null) season = parseNum(aplayS.textContent, 'season');
          if (aplayE && episode == null) episode = parseNum(aplayE.textContent, 'episode');
          document.querySelectorAll('.item-el.item-st').forEach((el) => {
            const t = (el?.textContent || '').trim();
            if (/сезон/i.test(t) && season == null) season = parseNum(t, 'season');
            else if (/серия|эпизод/i.test(t) && episode == null) episode = parseNum(t, 'episode');
          });
          if (season != null || episode != null) {
            return { season: season ?? null, episode: episode ?? null };
          }
          return null;
        }
      }
    },
    
    'kion.ru': {
      isSeries: () => {
        // Проверяем URL - если есть /serial/, это сериал
        const path = window.location.pathname || '';
        if (path.includes('/serial/')) return true;
        // Проверяем title - если есть слово "сериал"
        const title = document.querySelector('title');
        if (title?.textContent?.includes('сериал')) return true;
        // Проверяем URL на /video/.../sezon-.../seriya-... - это сериал
        if (path.match(/\/video\/[^/]+\/sezon-\d+\/seriya-\d+/)) return true;
        return false;
      },
      title: {
        selector: 'web-movie-card .card-header-title, player-web-ui-header h1.title, title',
        extract: (el) => {
          // Приоритет: card-header-title на странице фильма/сериала
          if (el?.classList?.contains?.('card-header-title')) {
            const text = el?.textContent?.trim() || '';
            return text || null;
          }
          // Приоритет: h1.title в плеере
          if (el?.classList?.contains?.('title') && el?.tagName === 'H1') {
            const text = el?.textContent?.trim() || '';
            return text || null;
          }
          // Fallback: title - извлекаем из кавычек "«Название»"
          const text = el?.textContent || '';
          const match = text.match(/«([^»]+)»/);
          if (match) return match[1].trim();
          // Или берем до скобки
          const beforeParen = text.split(/\s*\(/)[0]?.trim();
          return beforeParen || null;
        }
      },
      year: {
        selector: 'title',
        extract: (el) => {
          const text = el?.textContent || '';
          // Ищем год после слова "сериал" или "фильм": "сериал 2025" или "(2025)"
          const match = text.match(/(?:сериал|фильм)\s+(\d{4})/) || text.match(/\((\d{4})/);
          return match ? match[1] : null;
        }
      },
      seasonEpisode: {
        fromUrl: () => {
          // Приоритет: из URL /video/.../sezon-1/seriya-2
          const path = window.location.pathname || '';
          const seasonMatch = path.match(/sezon-(\d+)/);
          const episodeMatch = path.match(/seriya-(\d+)/);
          if (seasonMatch && episodeMatch) {
            return { season: parseInt(seasonMatch[1]), episode: parseInt(episodeMatch[1]) };
          }
          return null;
        },
        selector: 'player-web-ui-header .subtitle, web-card-content-buttons-redesign .caption-c1-medium-comp span',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          // Формат в плеере: "Сезон 1 | Серия 1"
          const m1 = t.match(/Сезон\s+(\d+)\s*\|\s*Серия\s+(\d+)/i);
          if (m1) {
            return { season: parseInt(m1[1]), episode: parseInt(m1[2]) };
          }
          // Формат на странице сериала: "с 2 серии 1 сезона" (серия, потом сезон!)
          const m2 = t.match(/(\d+)\s*серии.*?(\d+)\s*сезона/i);
          if (m2) {
            return { season: parseInt(m2[2]), episode: parseInt(m2[1]) };
          }
          // Обычный формат: "сезон 1 серия 2"
          const m3 = t.match(/(\d+)\s*сезон.*?(\d+)\s*серия/i) || t.match(/сезон\s*(\d+).*?серия\s*(\d+)/i);
          if (m3) {
            return { season: parseInt(m3[1]), episode: parseInt(m3[2]) };
          }
          return null;
        }
      }
    },

    'kino.pub': {
      isValidPage: () => {
        const path = (window.location.pathname || '').replace(/\/$/, '');
        if (/^\/movie(\?|$)/.test(path) || path.startsWith('/users/')) return false;
        return /\/item\/view\/\d+\/s\d+e\d+/.test(path);
      },
      isSeries: () => {
        const m = (window.location.pathname || '').match(/\/item\/view\/\d+\/s(\d+)e\d+/);
        return m ? parseInt(m[1], 10) > 0 : false;
      },
      title: {
        selector: '#view > div > div > h3',
        extract: (el) => {
          if (el) {
            const c = el.cloneNode(true);
            while (c.firstElementChild) c.removeChild(c.firstElementChild);
            const t = c.textContent?.replace(/\s+/g, ' ').trim();
            if (t) return t;
          }
          const fallback = document.querySelector('#player .player-title-main-ru');
          return fallback?.textContent?.trim() || null;
        }
      },
      year: {
        selector: '#view table tbody tr:nth-child(5) td:nth-child(2) a',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          return /^\d{4}$/.test(t) ? t : null;
        }
      },
      seasonEpisode: {
        fromUrl: () => {
          const m = (window.location.pathname || '').match(/\/item\/view\/\d+\/s(\d+)e(\d+)/);
          if (m) {
            return { season: parseInt(m[1], 10), episode: parseInt(m[2], 10) };
          }
          return null;
        }
      }
    },

    'smotreshka.tv': {
      isValidPage: () => {
        const path = (window.location.pathname || '').replace(/\/$/, '');
        if (path === '' || path === '/' || path === '/vod' || path === '/channels/now' || path === '/archive') return false;
        return path.startsWith('/vod/');
      },
      isSeries: () => {
        const modalEl = document.querySelector('#modal .title-duration.color-dark-font-secondary span');
        if (modalEl && /сезон|сезона|сезоны|сезонов|сезону/i.test(modalEl.textContent || '')) return true;
        const el = document.querySelector('.player-vod .episode.now .marquee-wrap span span');
        const t = el?.textContent?.trim() || '';
        return /Сезон\s+\d+/i.test(t);
      },
      title: {
        selector: '#modal .header.fixed .title, #modal .header.fixed > div, #scroll-container main .player-vod .player-footer-info .schedule-line.flex-space-between.flex-nowrap div span span',
        extract: (el) => el?.textContent?.trim() || null
      },
      year: {
        selector: '#modal .item.year',
        extract: (el) => {
          let t = el?.textContent?.trim() || '';
          if (!t) {
            const modal = document.querySelector('#modal');
            if (modal) {
              const yEl = modal.querySelector('.item.year');
              t = yEl?.textContent?.trim() || '';
            }
          }
          if (!t) return null;
          const m = t.match(/^(\d{4})/);
          if (m) return m[1];
          const m2 = t.match(/(\d{4})/);
          return m2 ? m2[1] : null;
        }
      },
      seasonEpisode: {
        selector: '.player-vod .episode.now .marquee-wrap span span',
        extract: (el) => {
          const t = el?.textContent?.trim() || '';
          const m = t.match(/Сезон\s+(\d+)/i);
          if (m) return { season: parseInt(m[1], 10), episode: null };
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
  
  function getUrlBase() {
    const path = (window.location.pathname || '').replace(/\/$/, '');
    if (hostname.includes('ivi.ru')) {
      const m = path.match(/\/watch\/([^/]+)/);
      return m ? `ivi:/watch/${m[1]}` : null;
    }
    if (hostname.includes('okko.tv')) {
      const m = path.match(/\/serial\/([^/]+)/);
      return m ? `okko:/serial/${m[1]}` : null;
    }
    if (hostname.includes('amediateka')) {
      const m = path.match(/\/watch\/([^/]+)/);
      return m ? `amediateka:/watch/${m[1]}` : null;
    }
    if (hostname.includes('kino.pub')) {
      const m = path.match(/\/item\/view\/(\d+)/);
      return m ? `kino:/item/view/${m[1]}` : null;
    }
    return null;
  }

  function isCatalogOrMainPage() {
    const path = (window.location.pathname || '').replace(/\/$/, '') || '/';
    const config = getSiteConfig();
    
    // Проверяем isValidPage из конфигурации (если есть)
    if (config?.isValidPage && typeof config.isValidPage === 'function') {
      if (!config.isValidPage()) return true;
    }
    
    if (hostname.includes('amediateka')) {
      // На главной странице виджет показываем только после клика на play
      if (path === '' || path === '/') {
        // Проверяем флаг playButtonClicked
        if (!config?.playButtonClicked) return true;
        return false; // После клика на play - показываем виджет
      }
      // Виджет только на /watch/... страницах
      if (!path.startsWith('/watch/')) return true; // Всё остальное - пропускаем
      return false;
    }
    if (hostname.includes('premier.one')) {
      // Главная страница - пропускаем
      if (path === '' || path === '/') return true;
      if (path.startsWith('/series') || path === '/movies' || path.startsWith('/movies')) return true;
      return false;
    }
    if (hostname.includes('hd.kinopoisk')) {
      if ((path || '').startsWith('/profiles')) return true;
      if (path === '' || path === '/') {
        const q = new URLSearchParams(window.location.search || '');
        if (!q.has('continueWatching') && !q.has('playingContentId')) return true;
      }
      return false;
    }
    if (hostname.includes('start.ru')) {
      // Виджет только на /watch/... страницах
      if (!path.startsWith('/watch/')) return true; // Всё остальное - пропускаем
      return false;
    }
    // OKKO: показывать виджет только на /movie/... и /serial/...
    if (hostname.includes('okko.tv')) {
      // Только страницы фильмов и сериалов
      if (!/^\/(movie|serial)\/[^/]+/.test(path)) return true;
      return false;
    }
    // PREMIER: показывать виджет только на /show/... (сериалы) и /film/... (фильмы)
    if (hostname.includes('premier.one')) {
      // Пропускаем каталоги: /all, /series, /movies, /collections и т.д.
      if (path === '' || path === '/' || path === '/all') return true;
      if (/^\/(series|movies|collections|promo|prems|channels)/.test(path)) return true;
      // Только страницы с контентом: /show/... или /film/...
      if (!/^\/(show|film)\/[^/]+/.test(path)) return true;
      return false;
    }
    // START: показывать виджет только на /watch/...
    if (hostname.includes('start.ru')) {
      // Пропускаем каталоги: /series, /movies, /movies/family и т.д.
      if (path === '' || path === '/' || path === '/auth') return true;
      if (/^\/(series|movies|films|collections|promo)/.test(path)) return true;
      // Только страницы просмотра: /watch/...
      if (!/^\/watch\/[^/]+/.test(path)) return true;
      return false;
    }
    // KION: показывать виджет только на /film/..., /serial/..., и /video/...
    if (hostname.includes('kion.ru')) {
      // Главная страница - пропускаем
      if (path === '' || path === '/') return true;
      // Только страницы фильмов, сериалов и просмотра
      if (!/^\/(film|serial|video)\//.test(path)) return true;
      return false;
    }
    return false;
  }
  
  function getSearchBaseTitle(info) {
    const config = getSiteConfig();
    const title = info?.title?.trim();
    if (!title) return null;
    if (config?.searchBaseTitle && typeof config.searchBaseTitle === 'function') {
      return config.searchBaseTitle(title) || title;
    }
    let base = title;
    // Убираем год в скобках: "Побег (2026)" → "Побег"
    base = base.replace(/\s*\(\d{4}\)\s*$/i, '').trim();
    // Убираем "— текст" после тире
    base = base.replace(/\s*—\s*[^(]+(\s*\([^)]*\))?\s*$/i, '').trim();
    // Убираем "(Часть X)"
    base = base.replace(/\s*\([^)]*[Чч]асть\s*\d+[^)]*\)\s*$/i, '').trim();
    // Убираем "(Сезон X)"
    base = base.replace(/\s*\([^)]*[Сс]езон\s*\d+[^)]*\)\s*$/i, '').trim();
    return base || title;
  }
  
  function getContentInfo() {
    const config = getSiteConfig();
    if (!config) return null;
    
    let title = null;
    let year = null;
    let seasonEpisode = null;
    let isSeries = false;
    
    if (config.title?.selector) {
      const el = document.querySelector(config.title.selector);
      if (el && config.title.extract) title = config.title.extract(el);
    }
    if (config.year?.selector) {
      const el = document.querySelector(config.year.selector);
      if (el && config.year.extract) year = config.year.extract(el);
    }
    if (config.seasonEpisode) {
      if (config.seasonEpisode.fromUrl) seasonEpisode = config.seasonEpisode.fromUrl();
      if (!seasonEpisode && typeof config.seasonEpisode.getSeasonEpisode === 'function') {
        seasonEpisode = config.seasonEpisode.getSeasonEpisode();
      }
      if (!seasonEpisode && config.seasonEpisode.selector) {
        const el = document.querySelector(config.seasonEpisode.selector);
        if (el && config.seasonEpisode.extract) seasonEpisode = config.seasonEpisode.extract(el);
      }
    }
    if (typeof config.isSeries === 'function') isSeries = config.isSeries();
    else isSeries = config.isSeries || !!seasonEpisode;
    
    const rawTitle = title || document.title.split(/[-|]/)[0].trim();
    return {
      title: rawTitle,
      year: year,
      season: seasonEpisode?.season || null,
      episode: seasonEpisode?.episode || null,
      isSeries: isSeries,
      url: window.location.href,
      url_base: getUrlBase()
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
  let hasTriggeredForCurrent = false; // один показ после 80% видео на серию
  
  // Кэш локальных данных (последние 100 просмотров)
  const CACHE_KEY = 'movieplanner_streaming_cache';
  const LAST_STREAMING_KEY = 'movieplanner_last_streaming_overlay';
  const MAX_CACHE_SIZE = 100;
  
  async function saveLastStreamingOverlay(info, filmData) {
    if (!info || !filmData?.kp_id || !info.season || !info.episode) return;
    const st = storageLocal();
    if (!st) return;
    try {
      await st.set({
        [LAST_STREAMING_KEY]: {
          hostname,
          url: info.url,
          title: info.title,
          year: info.year,
          season: info.season,
          episode: info.episode,
          kp_id: filmData.kp_id,
          film_id: filmData.film_id
        }
      });
    } catch (e) {
      if (isContextInvalidated(e)) return;
      console.error('[STREAMING] Ошибка сохранения lastStreamingOverlay:', e);
    }
  }
  
  async function getLocalCache() {
    const st = storageLocal();
    if (!st) return [];
    try {
      const data = await st.get([CACHE_KEY]);
      return data[CACHE_KEY] || [];
    } catch (e) {
      if (isContextInvalidated(e)) return [];
      console.error('[STREAMING] Ошибка получения кэша:', e);
      return [];
    }
  }
  
  async function saveToLocalCache(info, kpId) {
    const st = storageLocal();
    if (!st) return;
    try {
      const cache = await getLocalCache();
      const year = (info.year != null && info.year !== '') ? String(info.year) : null;
      cache.unshift({
        title: info.title,
        year: year,
        kp_id: kpId,
        url_base: info.url_base || null,
        hostname: hostname,
        timestamp: Date.now()
      });
      if (cache.length > MAX_CACHE_SIZE) cache.splice(MAX_CACHE_SIZE);
      await st.set({ [CACHE_KEY]: cache });
    } catch (e) {
      if (isContextInvalidated(e)) return;
      console.error('[STREAMING] Ошибка сохранения в кэш:', e);
    }
  }
  
  async function findInLocalCache(info) {
    try {
      const cache = await getLocalCache();
      const year = (info.year != null && info.year !== '') ? String(info.year) : null;
      const titleLower = (info.title || '').toLowerCase();
      let match = cache.find(item =>
        item.title?.toLowerCase() === titleLower && String(item.year || '') === String(year || '')
      );
      if (match) return match.kp_id;
      if (info.url_base && year) {
        match = cache.find(item =>
          item.url_base === info.url_base && item.hostname === hostname && String(item.year || '') === year
        );
        if (match) return match.kp_id;
      }
      if (info.url_base) {
        match = cache.find(item => item.url_base === info.url_base && item.hostname === hostname);
        if (match) return match.kp_id;
      }
      return null;
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
      width: 280px;
      min-width: 280px;
      max-width: 280px;
      max-height: 70vh;
      overflow-y: auto;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 14px;
      line-height: 1.4;
      pointer-events: auto;
      box-sizing: border-box;
      cursor: move;
      user-select: none;
      display: block !important;
      visibility: visible !important;
      opacity: 1 !important;
    `;
    
    // Устанавливаем позицию (обязательно указываем и left/right, и top/bottom)
    // Проверяем, что значения валидные (не null, не undefined, не NaN)
    // И что виджет не выходит за пределы экрана
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const widgetWidth = 280; // Фиксированная ширина виджета
    const minMargin = 10;
    
    let useLeft = false;
    let leftVal = 0;
    let rightVal = 20;
    
    if (savedPos && typeof savedPos.left === 'number' && !isNaN(savedPos.left)) {
      // Проверяем, что виджет не выходит за пределы
      if (savedPos.left >= 0 && savedPos.left + widgetWidth <= viewportWidth - minMargin) {
        useLeft = true;
        leftVal = savedPos.left;
        console.log('[STREAMING] Установлена позиция left:', savedPos.left);
      } else {
        console.log('[STREAMING] Сохранённая позиция left выходит за экран, используем default');
      }
    } else if (savedPos && typeof savedPos.right === 'number' && !isNaN(savedPos.right)) {
      if (savedPos.right >= 0 && savedPos.right <= viewportWidth - widgetWidth - minMargin) {
        rightVal = savedPos.right;
        console.log('[STREAMING] Установлена позиция right:', savedPos.right);
      } else {
        console.log('[STREAMING] Сохранённая позиция right выходит за экран, используем default');
      }
    }
    
    if (useLeft) {
      initialStyle += `left: ${leftVal}px !important; right: auto !important;`;
    } else {
      initialStyle += `right: ${rightVal}px !important; left: auto !important;`;
    }
    
    let useTop = false;
    let topVal = 0;
    let bottomVal = 20;
    
    if (savedPos && typeof savedPos.top === 'number' && !isNaN(savedPos.top)) {
      if (savedPos.top >= 0 && savedPos.top <= viewportHeight - 100) {
        useTop = true;
        topVal = savedPos.top;
        console.log('[STREAMING] Установлена позиция top:', savedPos.top);
      } else {
        console.log('[STREAMING] Сохранённая позиция top выходит за экран, используем default');
      }
    } else if (savedPos && typeof savedPos.bottom === 'number' && !isNaN(savedPos.bottom)) {
      if (savedPos.bottom >= 0 && savedPos.bottom <= viewportHeight - 100) {
        bottomVal = savedPos.bottom;
        console.log('[STREAMING] Установлена позиция bottom:', savedPos.bottom);
      } else {
        console.log('[STREAMING] Сохранённая позиция bottom выходит за экран, используем default');
      }
    }
    
    if (useTop) {
      initialStyle += `top: ${topVal}px !important; bottom: auto !important;`;
    } else {
      initialStyle += `bottom: ${bottomVal}px !important; top: auto !important;`;
    }
    
    overlayElement.style.cssText = initialStyle;
    
    let safeTitle = (info.title || '').replace(/\s*\(\d{4}\)\s*$/, '').trim();
    const yearPart = info.year ? ` (${info.year})` : '';
    let titleText;
    if (info.isSeries) {
      if (info.noEpisodeDetected) {
        titleText = `${safeTitle}${yearPart} (сериал)`;
      } else {
        titleText = `${safeTitle}${yearPart} - ${info.season || '?'} сезон, ${info.episode || '?'} серия`;
      }
    } else {
      titleText = `${safeTitle}${yearPart}`;
    }
    
    overlayElement.innerHTML = `
      <div style="margin-bottom: 12px !important; padding-right: 24px !important;">
        <strong style="font-size: 16px !important; display: block !important;">🎬 Movie Planner</strong>
        <div style="margin-top: 8px !important; opacity: 0.9 !important; font-size: 13px !important; line-height: 1.3 !important;">${titleText}</div>
      </div>
      <div id="mpp-buttons-container" style="display: flex !important; flex-direction: column !important; gap: 8px !important; width: 100% !important;"></div>
      <button id="mpp-close" style="position: absolute !important; top: 8px !important; right: 8px !important; background: rgba(255,255,255,0.2) !important; border: none !important; color: white !important; width: 24px !important; height: 24px !important; border-radius: 50% !important; cursor: pointer !important; font-size: 18px !important; line-height: 1 !important; display: flex !important; align-items: center !important; justify-content: center !important; outline: none !important; box-shadow: none !important;">×</button>
    `;
    
    document.body.appendChild(overlayElement);
    console.log('[STREAMING] Overlay добавлен в DOM:', overlayElement);
    
    overlayElement.querySelector('#mpp-close').addEventListener('click', async (e) => {
      e.stopPropagation();
      await saveLastStreamingOverlay(currentInfo, currentFilmData);
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
    
    await renderButtons(info, filmData);
    await saveLastStreamingOverlay(info, filmData);
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
    const isUnknown = filmId === undefined;
    const showSeriesUi = !!(info.isSeries && (filmData?.is_series === undefined || filmData?.is_series === true));
    const noEpisodeDetected = !!info.noEpisodeDetected;
    
    console.log('[STREAMING] renderButtons: isInDatabase=', isInDatabase, 'isUnknown=', isUnknown, 'film_id=', filmId, 'showSeriesUi=', showSeriesUi, 'noEpisodeDetected=', noEpisodeDetected);
    
    const st = storageLocal();
    const storageData = st ? await st.get(['has_notifications_access']) : {};
    const hasNotificationsAccess = storageData.has_notifications_access || false;
    const hasSeriesFeaturesAccess = filmData?.has_series_features_access ?? hasNotificationsAccess;
    const showSeriesButtonsAlways = true;
    
    // Если сериал без определенной серии - показываем простой UI
    if (showSeriesUi && noEpisodeDetected) {
      if (!isInDatabase) {
        // Сериал НЕ в базе - показываем только кнопку "Добавить в базу"
        const addBtn = document.createElement('button');
        addBtn.textContent = '➕ ДОБАВИТЬ В БАЗУ';
        addBtn.style.cssText = `
          width: 100% !important;
          padding: 12px 16px !important;
          background: white !important;
          color: #667eea !important;
          border: none !important;
          border-radius: 8px !important;
          font-weight: 700 !important;
          cursor: pointer !important;
          font-size: 14px !important;
          letter-spacing: 0.5px !important;
          box-sizing: border-box !important;
          text-align: center !important;
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          min-height: 44px !important;
          line-height: 1.2 !important;
        `;
        addBtn.addEventListener('click', () => handleAddToDatabase(info, filmData));
        container.appendChild(addBtn);
      } else {
        // Сериал УЖЕ в базе - показываем следующую непросмотренную серию
        const nextSeason = filmData?.next_unwatched_season || 1;
        const nextEpisode = filmData?.next_unwatched_episode || 1;
        
        if (showSeriesButtonsAlways || hasSeriesFeaturesAccess) {
          // Информация о следующей серии (кнопки всегда видны, при лимите покажем toast при ошибке)
          const nextEpInfo = document.createElement('div');
          nextEpInfo.style.cssText = 'padding: 12px !important; background: rgba(255,255,255,0.15) !important; border-radius: 8px !important; text-align: center !important; margin-bottom: 10px !important;';
          nextEpInfo.innerHTML = `<span style="font-size: 12px; opacity: 0.9;">Следующая серия:</span><br><b style="font-size: 16px;">${nextSeason} сезон, ${nextEpisode} серия</b>`;
          container.appendChild(nextEpInfo);
          
          // Кнопка отметки текущей серии
          const markBtn = document.createElement('button');
          markBtn.textContent = `✅ Отметить ${nextSeason}×${nextEpisode}`;
          markBtn.style.cssText = `
            width: 100% !important;
            padding: 12px 16px !important;
            background: white !important;
            color: #667eea !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 700 !important;
            cursor: pointer !important;
            font-size: 14px !important;
            box-sizing: border-box !important;
            text-align: center !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            min-height: 44px !important;
            line-height: 1.2 !important;
          `;
          markBtn.addEventListener('click', async () => {
            const manualInfo = { ...info, season: nextSeason, episode: nextEpisode, noEpisodeDetected: false };
            await handleMarkEpisode(manualInfo, filmData, false);
          });
          container.appendChild(markBtn);
          
          // Кнопка "Отметить все до этой" только если есть непросмотренные до следующей серии (и это не 1×1)
          if ((nextSeason > 1 || nextEpisode > 1) && filmData.has_unwatched_before) {
            const markAllBtn = document.createElement('button');
            markAllBtn.textContent = '✅ Отметить все до этой';
            markAllBtn.style.cssText = `
              width: 100% !important;
              padding: 10px 16px !important;
              background: rgba(255,255,255,0.2) !important;
              color: white !important;
              border: 1px solid rgba(255,255,255,0.3) !important;
              border-radius: 8px !important;
              font-weight: 600 !important;
              cursor: pointer !important;
              font-size: 13px !important;
              box-sizing: border-box !important;
              text-align: center !important;
              display: flex !important;
              align-items: center !important;
              justify-content: center !important;
              min-height: 40px !important;
              line-height: 1.2 !important;
            `;
            markAllBtn.addEventListener('click', async () => {
              const manualInfo = { ...info, season: nextSeason, episode: nextEpisode, noEpisodeDetected: false };
              await handleMarkEpisode(manualInfo, filmData, true);
            });
            container.appendChild(markAllBtn);
          }
        } else {
          // Нет подписки - только информация
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px !important; background: rgba(255,255,255,0.1) !important; border-radius: 8px !important; text-align: center !important; font-size: 12px !important;';
          noAccessMsg.innerHTML = '🔒 Для отметки серий нужна подписка';
          container.appendChild(noAccessMsg);
        }
      }
      return;
    }
    
    // Если ФИЛЬМ без определенных данных (lordfilm) - показываем простой UI
    if (!showSeriesUi && noEpisodeDetected) {
      if (!isInDatabase) {
        // Фильм НЕ в базе - показываем кнопку "Добавить в базу"
        const addBtn = document.createElement('button');
        addBtn.textContent = '➕ ДОБАВИТЬ В БАЗУ';
        addBtn.style.cssText = `
          width: 100% !important;
          padding: 12px 16px !important;
          background: white !important;
          color: #667eea !important;
          border: none !important;
          border-radius: 8px !important;
          font-weight: 700 !important;
          cursor: pointer !important;
          font-size: 14px !important;
          letter-spacing: 0.5px !important;
          box-sizing: border-box !important;
          text-align: center !important;
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          min-height: 44px !important;
          line-height: 1.2 !important;
        `;
        addBtn.addEventListener('click', () => handleAddToDatabase(info, filmData));
        container.appendChild(addBtn);
      } else {
        // Фильм УЖЕ в базе
        if (!filmData?.watched) {
          // Не просмотрен - предлагаем отметить
          const markBtn = document.createElement('button');
          markBtn.textContent = '✅ Отметить просмотренным';
          markBtn.style.cssText = `
            width: 100% !important;
            padding: 12px 16px !important;
            background: white !important;
            color: #667eea !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 700 !important;
            cursor: pointer !important;
            font-size: 14px !important;
            box-sizing: border-box !important;
            text-align: center !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            min-height: 44px !important;
            line-height: 1.2 !important;
          `;
          markBtn.addEventListener('click', () => handleMarkFilmWatched(info, filmData));
          container.appendChild(markBtn);
        } else if (!filmData?.rated) {
          // Просмотрен, но не оценен - предлагаем оценить
          const rateInfo = document.createElement('div');
          rateInfo.style.cssText = 'padding: 10px !important; background: rgba(255,255,255,0.1) !important; border-radius: 8px !important; text-align: center !important; margin-bottom: 10px !important; font-size: 12px !important;';
          rateInfo.innerHTML = '✓ Фильм просмотрен';
          container.appendChild(rateInfo);
          
          const rateBtn = document.createElement('button');
          rateBtn.textContent = '⭐ Оценить фильм';
          rateBtn.style.cssText = `
            width: 100% !important;
            padding: 12px !important;
            background: white !important;
            color: #667eea !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 700 !important;
            cursor: pointer !important;
            font-size: 14px !important;
            box-sizing: border-box !important;
          `;
          rateBtn.addEventListener('click', () => showRatingButtons(info, filmData));
          container.appendChild(rateBtn);
        } else {
          // Уже и просмотрен, и оценен
          const doneInfo = document.createElement('div');
          doneInfo.style.cssText = 'padding: 12px !important; background: rgba(255,255,255,0.1) !important; border-radius: 8px !important; text-align: center !important; font-size: 13px !important;';
          doneInfo.innerHTML = '✓ Фильм просмотрен и оценен';
          container.appendChild(doneInfo);
        }
      }
      return;
    }
    
    if (isUnknown && filmData?.kp_id) {
      if (showSeriesUi) {
        if (!hasSeriesFeaturesAccess && !showSeriesButtonsAlways) {
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px; background: rgba(255,255,255,0.1); border-radius: 6px; text-align: center; font-size: 13px; margin-bottom: 8px;';
          noAccessMsg.innerHTML = '🔒 Для отметки серий нужна подписка 💎 Movie Planner PRO<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else {
          // Определяем целевую серию: если текущая просмотрена, показываем следующую непросмотренную
          let targetSeason = info.season;
          let targetEpisode = info.episode;
          if (filmData.current_episode_watched) {
            if (filmData.next_unwatched_season != null && filmData.next_unwatched_episode != null) {
              targetSeason = filmData.next_unwatched_season;
              targetEpisode = filmData.next_unwatched_episode;
              console.log('[STREAMING] Текущая серия просмотрена, показываем следующую:', targetSeason, targetEpisode);
            } else if (info.season != null && info.episode != null) {
              targetSeason = info.season;
              targetEpisode = (info.episode || 0) + 1;
              console.log('[STREAMING] Текущая серия просмотрена, fallback следующая:', targetSeason, targetEpisode);
            }
          }
          
          // Если открытая серия уже отмечена — только текст, без кнопок
          if (filmData.current_episode_watched && targetSeason != null && targetEpisode != null) {
            const statusEl = document.createElement('div');
            statusEl.style.cssText = 'font-size: 12px; opacity: 0.95; margin-bottom: 8px;';
            statusEl.textContent = `Ближайшая непросмотренная серия — ${targetSeason}×${targetEpisode}`;
            container.appendChild(statusEl);
          } else if (targetSeason && targetEpisode) {
            const markCurrentBtn = document.createElement('button');
            markCurrentBtn.textContent = `✅ Отметить серию ${targetSeason}×${targetEpisode}`;
            markCurrentBtn.style.cssText = `
              width: 100% !important;
              padding: 12px 16px !important;
              background: white !important;
              color: #667eea !important;
              border: none !important;
              border-radius: 8px !important;
              font-weight: 700 !important;
              cursor: pointer !important;
              font-size: 14px !important;
              box-sizing: border-box !important;
              text-align: center !important;
              display: flex !important;
              align-items: center !important;
              justify-content: center !important;
              min-height: 44px !important;
              line-height: 1.2 !important;
            `;
            const targetInfo = { ...info, season: targetSeason, episode: targetEpisode };
            markCurrentBtn.addEventListener('click', () => handleMarkEpisode(targetInfo, filmData, false));
            container.appendChild(markCurrentBtn);
            
            const isTargetCurrentPage = (targetSeason === info.season && targetEpisode === info.episode);
            const hasUnwatchedBefore = filmData.has_unwatched_before &&
              (targetSeason > 1 || targetEpisode > 1) &&
              !(isTargetCurrentPage && !filmData.current_episode_watched);
            
            if (hasUnwatchedBefore) {
              const markAllBtn = document.createElement('button');
              markAllBtn.textContent = '✅ Отметить все предыдущие';
              markAllBtn.style.cssText = `
                width: 100% !important;
                padding: 10px 16px !important;
                background: rgba(255,255,255,0.2) !important;
                color: white !important;
                border: 1px solid rgba(255,255,255,0.3) !important;
                border-radius: 8px !important;
                font-weight: 600 !important;
                cursor: pointer !important;
                font-size: 13px !important;
                box-sizing: border-box !important;
                text-align: center !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                min-height: 40px !important;
                line-height: 1.2 !important;
              `;
              const targetInfoAll = { ...info, season: targetSeason, episode: targetEpisode };
              markAllBtn.addEventListener('click', () => handleMarkEpisode(targetInfoAll, filmData, true));
              container.appendChild(markAllBtn);
            }
          }
        }
      }
      return;
    }
    
    if (!isInDatabase) {
      // Фильм/сериал не в базе - показываем кнопку "Добавить в базу"
      const addBtn = document.createElement('button');
      addBtn.textContent = '➕ Добавить в базу';
      addBtn.style.cssText = `
        width: 100% !important;
        padding: 12px 16px !important;
        background: white !important;
        color: #667eea !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 700 !important;
        cursor: pointer !important;
        font-size: 14px !important;
        box-sizing: border-box !important;
        text-align: center !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        min-height: 44px !important;
        line-height: 1.2 !important;
      `;
      addBtn.addEventListener('click', () => handleAddToDatabase(info, filmData));
      container.appendChild(addBtn);
    } else {
      if (showSeriesUi) {
        if (!hasSeriesFeaturesAccess && !showSeriesButtonsAlways) {
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px; background: rgba(255,255,255,0.1); border-radius: 6px; text-align: center; font-size: 13px; margin-bottom: 8px;';
          noAccessMsg.innerHTML = '🔒 Для отметки серий нужна подписка 💎 Movie Planner PRO<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else {
          // Определяем целевую серию: если текущая просмотрена, показываем следующую непросмотренную
          let targetSeason = info.season;
          let targetEpisode = info.episode;
          if (filmData.current_episode_watched) {
            if (filmData.next_unwatched_season != null && filmData.next_unwatched_episode != null) {
              targetSeason = filmData.next_unwatched_season;
              targetEpisode = filmData.next_unwatched_episode;
              console.log('[STREAMING] Текущая серия просмотрена, показываем следующую:', targetSeason, targetEpisode);
            } else if (info.season != null && info.episode != null) {
              targetSeason = info.season;
              targetEpisode = (info.episode || 0) + 1;
              console.log('[STREAMING] Текущая серия просмотрена, fallback следующая:', targetSeason, targetEpisode);
            }
          }
          
          // Если открытая серия уже отмечена — только текст, без кнопок
          if (filmData.current_episode_watched && targetSeason != null && targetEpisode != null) {
            const statusEl = document.createElement('div');
            statusEl.style.cssText = 'font-size: 12px; opacity: 0.95; margin-bottom: 8px;';
            statusEl.textContent = `Ближайшая непросмотренная серия — ${targetSeason}×${targetEpisode}`;
            container.appendChild(statusEl);
          } else if (targetSeason && targetEpisode) {
            const markCurrentBtn = document.createElement('button');
            markCurrentBtn.textContent = `✅ Отметить серию ${targetSeason}×${targetEpisode}`;
            markCurrentBtn.style.cssText = `
              width: 100% !important;
              padding: 12px 16px !important;
              background: white !important;
              color: #667eea !important;
              border: none !important;
              border-radius: 8px !important;
              font-weight: 700 !important;
              cursor: pointer !important;
              font-size: 14px !important;
              box-sizing: border-box !important;
              text-align: center !important;
              display: flex !important;
              align-items: center !important;
              justify-content: center !important;
              min-height: 44px !important;
              line-height: 1.2 !important;
            `;
            const targetInfo = { ...info, season: targetSeason, episode: targetEpisode };
            markCurrentBtn.addEventListener('click', () => handleMarkEpisode(targetInfo, filmData, false));
            container.appendChild(markCurrentBtn);
            
            const isTargetCurrentPage = (targetSeason === info.season && targetEpisode === info.episode);
            const hasUnwatchedBefore = filmData.has_unwatched_before &&
              (targetSeason > 1 || targetEpisode > 1) &&
              !(isTargetCurrentPage && !filmData.current_episode_watched);
            
            if (hasUnwatchedBefore) {
              const markAllBtn = document.createElement('button');
              markAllBtn.textContent = '✅ Отметить все предыдущие';
              markAllBtn.style.cssText = `
                width: 100% !important;
                padding: 10px 16px !important;
                background: rgba(255,255,255,0.2) !important;
                color: white !important;
                border: 1px solid rgba(255,255,255,0.3) !important;
                border-radius: 8px !important;
                font-weight: 600 !important;
                cursor: pointer !important;
                font-size: 13px !important;
                box-sizing: border-box !important;
                text-align: center !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                min-height: 40px !important;
                line-height: 1.2 !important;
              `;
              const targetInfoAll = { ...info, season: targetSeason, episode: targetEpisode };
              markAllBtn.addEventListener('click', () => handleMarkEpisode(targetInfoAll, filmData, true));
              container.appendChild(markAllBtn);
            }
          }
        }
      } else {
        // Фильм
        if (!hasNotificationsAccess) {
          // Нет подписки - показываем только информацию
          const noAccessMsg = document.createElement('div');
          noAccessMsg.style.cssText = 'padding: 12px !important; background: rgba(255,255,255,0.1) !important; border-radius: 8px !important; text-align: center !important; font-size: 13px !important;';
          noAccessMsg.innerHTML = '🔒 Для отметки фильмов нужна подписка 💎 Movie Planner PRO<br><small style="opacity: 0.8;">Доступно только добавление в базу</small>';
          container.appendChild(noAccessMsg);
        } else {
          // Есть подписка - показываем кнопки
          if (!filmData.watched) {
            const markWatchedBtn = document.createElement('button');
            markWatchedBtn.textContent = '✅ Отметить как просмотренный';
            markWatchedBtn.style.cssText = `
              width: 100% !important;
              padding: 12px 16px !important;
              background: white !important;
              color: #667eea !important;
              border: none !important;
              border-radius: 8px !important;
              font-weight: 700 !important;
              cursor: pointer !important;
              font-size: 14px !important;
              box-sizing: border-box !important;
              text-align: center !important;
              display: flex !important;
              align-items: center !important;
              justify-content: center !important;
              min-height: 44px !important;
              line-height: 1.2 !important;
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
    ratingContainer.style.cssText = 'display: flex; gap: 2px; flex-wrap: nowrap; margin-bottom: 8px;';
    
    for (let i = 1; i <= 10; i++) {
      const btn = document.createElement('button');
      btn.textContent = '⭐';
      btn.dataset.rating = i;
      btn.style.cssText = `
        flex: 1 1 0 !important;
        min-width: 0 !important;
        height: 32px !important;
        background: rgba(255,255,255,0.2) !important;
        color: white !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        border-radius: 6px !important;
        cursor: pointer !important;
        font-size: 14px !important;
        transition: all 0.2s !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        padding: 0 !important;
        line-height: 1 !important;
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
  function alertReloadPage() {
    try { alert('Расширение обновилось. Обновите страницу (F5).'); } catch (_) {}
  }
  
  // Показать временное уведомление (toast) вместо alert
  function showToast(message, duration = 2500) {
    // Удаляем предыдущий toast если есть
    const existingToast = document.getElementById('mpp-toast');
    if (existingToast) existingToast.remove();
    
    const toast = document.createElement('div');
    toast.id = 'mpp-toast';
    toast.textContent = message;
    toast.style.cssText = `
      position: fixed;
      bottom: 100px;
      left: 50%;
      transform: translateX(-50%);
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 12px 24px;
      border-radius: 8px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 14px;
      font-weight: 600;
      z-index: 999999;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
      opacity: 0;
      transition: opacity 0.3s ease;
      pointer-events: none;
    `;
    document.body.appendChild(toast);
    
    // Показываем
    setTimeout(() => { toast.style.opacity = '1'; }, 10);
    
    // Скрываем и удаляем
    setTimeout(() => {
      toast.style.opacity = '0';
      setTimeout(() => toast.remove(), 300);
    }, duration);
  }

  async function handleAddToDatabase(info, filmData) {
    try {
      const st = storageLocal();
      if (!st) { alertReloadPage(); return; }
      let data;
      try {
        data = await st.get(['linked_chat_id', 'linked_user_id']);
      } catch (se) {
        if (isContextInvalidated(se)) { alertReloadPage(); return; }
        throw se;
      }
      if (!data.linked_chat_id) {
        alert('Сначала привяжите аккаунт в расширении');
        return;
      }
      if (!filmData?.kp_id) {
        alert('Поиск фильма... (это займет несколько секунд)');
        return;
      }
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
            // Обновляем filmData с новым film_id
            // Для сериала устанавливаем следующую непросмотренную серию = 1×1
            const hasUnwatchedBefore = info.isSeries && info.season && info.episode && (info.season > 1 || info.episode > 1);
            currentFilmData = { 
              ...filmData, 
              film_id: result.film_id, 
              kp_id: filmData.kp_id,
              has_unwatched_before: hasUnwatchedBefore,
              current_episode_watched: false,
              watched: false,
              rated: false,
              // Для только что добавленного сериала - следующая серия 1×1
              next_unwatched_season: info.isSeries ? 1 : undefined,
              next_unwatched_episode: info.isSeries ? 1 : undefined,
              is_series: info.isSeries
            };
            currentInfo = info;
            showToast('✅ Добавлено в базу!');
            await renderButtons(info, currentFilmData);
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          alert('Ошибка сервера: ' + response.status);
        }
      } catch (fetchError) {
        if (isContextInvalidated(fetchError)) { alertReloadPage(); return; }
        console.error('[STREAMING] Ошибка fetch при добавлении в базу:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      if (isContextInvalidated(e)) { alertReloadPage(); return; }
      console.error('[STREAMING] Ошибка добавления в базу:', e);
      alert('Ошибка добавления в базу: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  async function handleMarkEpisode(info, filmData, markAllPrevious) {
    try {
      const st = storageLocal();
      if (!st) { alertReloadPage(); return; }
      let data;
      try {
        data = await st.get(['linked_chat_id', 'linked_user_id']);
      } catch (se) {
        if (isContextInvalidated(se)) { alertReloadPage(); return; }
        throw se;
      }
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
            showToast('✅ Серия отмечена!');
            removeOverlay();
          } else {
            const errMsg = result.message || result.error || 'Ошибка';
            showToast('❌ ' + errMsg, 3000);
          }
        } else {
          let result = {};
          try { result = await response.json(); } catch (_) {}
          const isLimit = response.status === 403 && result.error === 'series_limit';
          showToast(isLimit ? '❌ Ошибка отметки, проверьте подписку' : ('❌ ' + (result.message || 'Ошибка сервера')), 3000);
        }
      } catch (fetchError) {
        if (isContextInvalidated(fetchError)) { alertReloadPage(); return; }
        const msg = (fetchError && fetchError.message) ? String(fetchError.message) : '';
        const is502or503 = /502|503/.test(msg);
        if (is502or503) {
          showToast('❌ Сервер временно недоступен. Попробуйте позже.', 4000);
        } else {
          console.error('[STREAMING] Ошибка fetch при отметке серии:', fetchError);
          showToast('❌ Ошибка подключения', 3000);
        }
      }
    } catch (e) {
      if (isContextInvalidated(e)) { alertReloadPage(); return; }
      console.error('[STREAMING] Ошибка отметки серии:', e);
      showToast('❌ Ошибка: ' + (e.message || 'неизвестная'), 3000);
    }
  }

  async function handleMarkFilmWatched(info, filmData) {
    try {
      const st = storageLocal();
      if (!st) { alertReloadPage(); return; }
      let data;
      try {
        data = await st.get(['linked_chat_id', 'linked_user_id']);
      } catch (se) {
        if (isContextInvalidated(se)) { alertReloadPage(); return; }
        throw se;
      }
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
            // Обновляем данные из API для получения актуального состояния
            try {
              const filmInfoUrl = `/api/extension/film-info?kp_id=${filmData.kp_id}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`;
              const filmInfoResponse = await apiRequest('GET', filmInfoUrl);
              if (filmInfoResponse.ok) {
                const filmInfoResult = await filmInfoResponse.json();
                if (filmInfoResult.film) {
                  currentFilmData = {
                    ...filmData,
                    watched: true,
                    rated: filmInfoResult.film.rated || false
                  };
                  renderButtons(info, currentFilmData);
                  // Если фильм не оценен, показываем оценку
                  if (!currentFilmData.rated) {
                    setTimeout(() => {
                      showRatingButtons(info, currentFilmData);
                    }, 500);
                  }
                  return;
                }
              }
            } catch (e) {
              console.error('[STREAMING] Ошибка получения обновленных данных:', e);
            }
            // Fallback если не удалось получить обновленные данные
            currentFilmData = { ...filmData, watched: true };
            renderButtons(info, currentFilmData);
            // Показываем оценку если фильм не оценен
            if (!filmData.rated) {
              setTimeout(() => {
                showRatingButtons(info, currentFilmData);
              }, 500);
            }
          } else {
            alert('Ошибка: ' + (result.error || 'неизвестная ошибка'));
          }
        } else {
          alert('Ошибка сервера: ' + response.status);
        }
      } catch (fetchError) {
        if (isContextInvalidated(fetchError)) { alertReloadPage(); return; }
        console.error('[STREAMING] Ошибка fetch при отметке фильма:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      if (isContextInvalidated(e)) { alertReloadPage(); return; }
      console.error('[STREAMING] Ошибка отметки фильма:', e);
      alert('Ошибка отметки фильма: ' + (e.message || 'неизвестная ошибка'));
    }
  }
  
  async function handleRating(info, filmData, rating) {
    try {
      const st = storageLocal();
      if (!st) { alertReloadPage(); return; }
      let data;
      try {
        data = await st.get(['linked_chat_id', 'linked_user_id']);
      } catch (se) {
        if (isContextInvalidated(se)) { alertReloadPage(); return; }
        throw se;
      }
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
        if (isContextInvalidated(fetchError)) { alertReloadPage(); return; }
        console.error('[STREAMING] Ошибка fetch при оценке:', fetchError);
        alert('Ошибка подключения к серверу. Проверьте интернет-соединение.');
      }
    } catch (e) {
      if (isContextInvalidated(e)) { alertReloadPage(); return; }
      console.error('[STREAMING] Ошибка оценки:', e);
      alert('Ошибка оценки: ' + (e.message || 'неизвестная ошибка'));
    }
  }

  // ────────────────────────────────────────────────
  // Основная логика проверки и показа плашки
  // ────────────────────────────────────────────────
  async function checkAndShowOverlay() {
    removeOverlay();
    if (isCatalogOrMainPage()) {
      console.log('[STREAMING] Пропуск: каталог или главная, не отправляем запросы');
      return;
    }
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
    
    // Для сериалов без сезона/серии: показываем виджет с предложением добавить в базу
    // и выбрать серию вручную. Если сезон/серия определены - показываем обычный виджет
    // Для фильмов: показываем всегда (если есть title)
    if (info.isSeries && (!info.season || !info.episode)) {
      console.log('[STREAMING] Сериал без сезона/серии - показываем упрощенный виджет');
      // Продолжаем выполнение, но флаг info.noEpisodeDetected = true
      info.noEpisodeDetected = true;
    }
    
    // Проверяем защиту от спама
    const shouldShow = shouldShowOverlay(info);
    console.log('[STREAMING] shouldShowOverlay результат:', shouldShow);
    
    if (!shouldShow) {
      console.log('[STREAMING] Пропуск: защита от спама');
      return;
    }
    
    try {
      const st = storageLocal();
      if (!st) return;
      let data;
      try {
        data = await st.get(['linked_chat_id', 'linked_user_id', 'has_notifications_access']);
      } catch (se) {
        if (isContextInvalidated(se)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
        throw se;
      }
      if (!data.linked_chat_id) {
        return; // Пользователь не привязан
      }
      
      if (data.has_notifications_access === undefined) {
        try {
          const subResponse = await apiRequest('GET', `/api/extension/check-subscription?chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}`);
          if (subResponse.ok) {
            const subResult = await subResponse.json();
            if (subResult.success) {
              await st.set({ has_notifications_access: subResult.has_notifications_access || false });
              data.has_notifications_access = subResult.has_notifications_access || false;
            }
          }
        } catch (subErr) {
          if (isContextInvalidated(subErr)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
          console.error('[STREAMING] Ошибка проверки подписки:', subErr);
          data.has_notifications_access = false;
        }
      }

      // Функция нормализации названия для сравнения
      function normalizeTitle(title) {
        if (!title) return '';
        return title.toLowerCase()
          .replace(/[ёЁ]/g, 'е')
          .replace(/\s+/g, ' ')
          .trim();
      }
      
      // Проверка, совпадает ли название из API с названием на странице
      function titlesMatch(pageTitle, apiTitle) {
        const normPage = normalizeTitle(pageTitle);
        const normApi = normalizeTitle(apiTitle);
        return normPage === normApi || 
               normPage.includes(normApi) || 
               normApi.includes(normPage);
      }

      // Сначала проверяем локальный кэш
      let kpId = await findInLocalCache(info);
      let filmData = null;
      let cacheValid = true;
      
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
              // Проверяем, совпадает ли название из API с названием на странице
              const apiTitle = result.film?.title || result.film?.nameRu || result.film?.name || result.film?.nameOriginal || '';
              const pageTitle = (getSearchBaseTitle(info) || info.title || '').trim();
              
              console.log('[STREAMING] Проверка совпадения названий. Страница:', pageTitle, 'API:', apiTitle);
              
              if (apiTitle && pageTitle && !titlesMatch(pageTitle, apiTitle)) {
                console.log('[STREAMING] Кэш невалиден: название не совпадает. Страница:', pageTitle, 'API:', apiTitle);
                cacheValid = false;
                kpId = null; // Сбрасываем, чтобы сделать новый поиск
              } else if (!apiTitle) {
                console.log('[STREAMING] Внимание: apiTitle пустой, пропускаем проверку');
              } else {
                // ВАЖНО: film_id может быть 0 или null, проверяем явно
                const filmId = (result.film_id !== undefined && result.film_id !== null) ? result.film_id : null;
                let nextS = result.next_unwatched_season;
                let nextE = result.next_unwatched_episode;
                if (result.current_episode_watched && (nextS == null || nextE == null) && info.season != null && info.episode != null) {
                  nextS = info.season;
                  nextE = (info.episode || 0) + 1;
                }
                filmData = {
                  kp_id: kpId,
                  film_id: filmId,
                  watched: result.watched || false,
                  rated: result.rated || false,
                  has_unwatched_before: result.has_unwatched_before || false,
                  current_episode_watched: result.current_episode_watched || false,
                  next_unwatched_season: nextS,
                  next_unwatched_episode: nextE,
                  is_series: !!result.film?.is_series,
                  has_series_features_access: !!result.has_series_features_access
                };
                console.log('[STREAMING] filmData после парсинга:', filmData);
              }
            } else {
              console.error('[STREAMING] API вернул success: false:', result);
            }
          } else {
            console.error('[STREAMING] HTTP ошибка:', response.status);
          }
        } catch (fetchError) {
          if (isContextInvalidated(fetchError)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
          console.error('[STREAMING] Ошибка fetch film-info:', fetchError);
          if (kpId) {
            console.log('[STREAMING] Повторная попытка запроса film-info для kp_id:', kpId);
            try {
              // Повторный запрос с таймаутом
              const retryResponse = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
              if (retryResponse.ok) {
                const retryResult = await retryResponse.json();
                if (retryResult.success) {
                  // Проверяем совпадение названия и в retry
                  const retryApiTitle = retryResult.film?.title || retryResult.film?.nameRu || retryResult.film?.name || retryResult.film?.nameOriginal || '';
                  const retryPageTitle = (getSearchBaseTitle(info) || info.title || '').trim();
                  
                  if (retryApiTitle && retryPageTitle && !titlesMatch(retryPageTitle, retryApiTitle)) {
                    console.log('[STREAMING] Retry: кэш невалиден, название не совпадает:', retryPageTitle, 'vs', retryApiTitle);
                    cacheValid = false;
                    kpId = null;
                  } else {
                    const filmId = (retryResult.film_id !== undefined && retryResult.film_id !== null) ? retryResult.film_id : null;
                    let retryNextS = retryResult.next_unwatched_season;
                    let retryNextE = retryResult.next_unwatched_episode;
                    if (retryResult.current_episode_watched && (retryNextS == null || retryNextE == null) && info.season != null && info.episode != null) {
                      retryNextS = info.season;
                      retryNextE = (info.episode || 0) + 1;
                    }
                    filmData = {
                      kp_id: kpId,
                      film_id: filmId,
                      watched: retryResult.watched || false,
                      rated: retryResult.rated || false,
                      has_unwatched_before: retryResult.has_unwatched_before || false,
                      current_episode_watched: retryResult.current_episode_watched || false,
                      next_unwatched_season: retryNextS,
                      next_unwatched_episode: retryNextE,
                      is_series: !!retryResult.film?.is_series,
                      has_series_features_access: !!retryResult.has_series_features_access
                    };
                    console.log('[STREAMING] Повторный запрос успешен, film_id:', filmId);
                  }
                } else {
                  throw new Error(retryResult.error || 'Unknown error');
                }
              } else {
                throw new Error(`HTTP ${retryResponse.status}`);
              }
            } catch (retryError) {
              if (isContextInvalidated(retryError)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
              console.error('[STREAMING] Повторный запрос тоже упал:', retryError);
              // Предполагаем, что фильм может быть в базе, но мы не можем это проверить
              filmData = {
                kp_id: kpId,
                film_id: undefined,
                watched: false,
                rated: false,
                has_unwatched_before: false,
                current_episode_watched: false,
                is_series: true
              };
              console.log('[STREAMING] Продолжаем с kp_id, но film_id неизвестен:', kpId);
            }
          } else {
            // Нет kp_id - не показываем виджет
            console.log('[STREAMING] Пропуск: нет kp_id и ошибка film-info');
            return;
          }
        }
      }
      
      // Если кэш невалиден или kpId не найден - делаем поиск
      if (!kpId || !filmData) {
        const searchType = info.isSeries ? 'TV_SERIES' : 'FILM';
        const baseTitle = (getSearchBaseTitle(info) || info.title || '').trim();
        const yearParam = info.year ? `&year=${info.year}` : '';
        const searchKeyword = baseTitle;
        console.log('[STREAMING] Поиск (название + год, как /search и Letterboxd):', { keyword: searchKeyword, year: info.year, type: searchType });
        
        // Используем normalizeTitle и titlesMatch, определённые выше
        
        async function doSearch(keyw, yParam) {
          try {
            const r = await apiRequest('GET', `/api/extension/search-film-by-keyword?keyword=${encodeURIComponent(keyw)}${yParam}&type=${searchType}`);
            if (!r.ok) return null;
            const j = await r.json();
            return (j.success && j.kp_id) ? j : null;
          } catch (_) {
            return null;
          }
        }
        function buildFilmData(sr, fr) {
          const fid = (fr?.film_id != null) ? fr.film_id : null;
          const isSer = !!(fr?.film?.is_series ?? sr?.film?.is_series);
          let nextS = fr?.next_unwatched_season;
          let nextE = fr?.next_unwatched_episode;
          if (fr?.current_episode_watched && (nextS == null || nextE == null) && info.season != null && info.episode != null) {
            nextS = info.season;
            nextE = (info.episode || 0) + 1;
          }
          return {
            kp_id: kpId,
            film_id: fid,
            watched: fr?.watched || false,
            rated: fr?.rated || false,
            has_unwatched_before: fr?.has_unwatched_before || false,
            current_episode_watched: fr?.current_episode_watched || false,
            next_unwatched_season: nextS,
            next_unwatched_episode: nextE,
            is_series: isSer
          };
        }
        try {
          let searchResult = await doSearch(searchKeyword, yearParam);
          
          // Специальная проверка для IVI: если название не совпадает, пробуем без года
          if (searchResult && hostname.includes('ivi.ru')) {
            const resultTitle = searchResult.film?.nameRu || searchResult.film?.nameOriginal || '';
            if (!titlesMatch(baseTitle, resultTitle)) {
              console.log('[STREAMING] IVI: название не совпадает, пробуем без года. Страница:', baseTitle, 'Результат:', resultTitle);
              const searchWithoutYear = await doSearch(searchKeyword, '');
              if (searchWithoutYear) {
                const newResultTitle = searchWithoutYear.film?.nameRu || searchWithoutYear.film?.nameOriginal || '';
                if (titlesMatch(baseTitle, newResultTitle)) {
                  console.log('[STREAMING] IVI: нашли совпадение без года:', newResultTitle);
                  searchResult = searchWithoutYear;
                } else {
                  console.log('[STREAMING] IVI: и без года название не совпадает:', newResultTitle);
                }
              }
            }
          }
          
          if (!searchResult && info.year && searchKeyword) {
            console.log('[STREAMING] Повторная попытка поиска без года (как fallback)');
            searchResult = await doSearch(searchKeyword, '');
          }
          if (!searchResult || !searchResult.kp_id) {
            console.log('[STREAMING] Фильм не найден: keyword=' + searchKeyword + (info.year ? ' year=' + info.year : ''));
            return;
          }
          kpId = searchResult.kp_id;
          await saveToLocalCache(info, kpId);
          try {
            const filmResponse = await apiRequest('GET', `/api/extension/film-info?kp_id=${kpId}&chat_id=${data.linked_chat_id}&user_id=${data.linked_user_id}${info.season && info.episode ? `&season=${info.season}&episode=${info.episode}` : ''}`);
            const fr = filmResponse.ok ? await filmResponse.json() : null;
            filmData = buildFilmData(searchResult, fr?.success ? fr : null);
          } catch (e) {
            if (isContextInvalidated(e)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
            console.error('[STREAMING] Ошибка film-info после поиска:', e);
            filmData = buildFilmData(searchResult, null);
          }
        } catch (searchError) {
          if (isContextInvalidated(searchError)) { console.log('[STREAMING] Пропуск: context invalidated'); return; }
          console.error('[STREAMING] Ошибка поиска:', searchError);
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
          current_episode_watched: false,
          is_series: filmData?.is_series ?? info.isSeries
        };
      }
      
      // Показываем плашку (даже если были ошибки API, но kp_id есть)
      console.log('[STREAMING] Вызываем createOverlay с данными:', { info, filmData });
      await createOverlay(info, filmData);
      console.log('[STREAMING] createOverlay вызван');
      
    } catch (e) {
      if (isContextInvalidated(e)) {
        console.log('[STREAMING] Пропуск: context invalidated');
        return;
      }
      console.error('[STREAMING] Ошибка проверки:', e);
      try {
        const filmData = {
          kp_id: null,
          film_id: null,
          watched: false,
          rated: false,
          has_unwatched_before: false,
          current_episode_watched: false,
          is_series: false
        };
        await createOverlay(info, filmData);
      } catch (overlayError) {
        if (isContextInvalidated(overlayError)) return;
        console.error('[STREAMING] Ошибка создания плашки:', overlayError);
      }
    }
  }
  
  // ────────────────────────────────────────────────
  // Инициализация и наблюдение за изменениями
  // ────────────────────────────────────────────────
  
  // Обработчик fullscreen для захвата сезона/серии
  function handleFullscreenChange() {
    // При входе в fullscreen пытаемся повторно определить сезон/серию
    setTimeout(() => {
      const info = getContentInfo();
      console.log('[STREAMING] Fullscreen change, пытаемся обновить info:', info);
      if (info && info.title) {
        // Если ранее не определили сезон/серию, но теперь определили - обновляем
        if (info.season && info.episode && currentInfo?.noEpisodeDetected) {
          console.log('[STREAMING] Fullscreen: теперь определены сезон/серия:', info.season, info.episode);
          const key = getContentKey(info);
          lastShown[key] = 0; // Сбрасываем кулдаун
          lastContentHash = ''; // Сбрасываем хеш
          checkAndShowOverlay();
        }
      }
    }, 1500); // Даём время плееру обновить UI
  }
  
  // Наблюдатель за кликом на кнопку fullscreen (для сайтов где fullscreen API не работает)
  function setupFullscreenButtonObserver() {
    // Селекторы кнопок fullscreen для разных сайтов
    const fullscreenSelectors = [
      // HDRezka/allplay
      '#allplay [data-allplay="fullscreen"]',
      '#allplay .allplay__control[data-allplay="fullscreen"]',
      'button[data-allplay="fullscreen"]',
      // Buzzoola player
      '.controls-right button[aria-label*="экран"]',
      '.controls-right button[aria-label*="fullscreen"]',
      '[data-testid="fullscreen-btn"]',
      // Generic
      '.player-fullscreen-button',
      '[class*="fullscreen"]',
      'button[title*="Полноэкранный"]',
      'button[title*="fullscreen"]'
    ];
    
    document.addEventListener('click', (e) => {
      const target = e.target.closest(fullscreenSelectors.join(', '));
      if (target) {
        console.log('[STREAMING] Клик на кнопку fullscreen');
        handleFullscreenChange();
      }
    }, true);
  }
  
  function init() {
    // Отслеживание клика на play кнопку на amediateka.ru (главная страница)
    if (hostname.includes('amediateka')) {
      const playButtonSelectors = [
        '.CardItem_cardItemLink__V3AQE .CardItem_play__0vEuz',
        '.CardItem_play__0vEuz',
        '[class*="CardItem_play"]',
        'svg[viewBox="0 0 48 48"]', // SVG play кнопка
        'img[alt="Play"]'
      ];
      
      document.addEventListener('click', (e) => {
        const target = e.target.closest(playButtonSelectors.join(', ')) || 
                       e.target.closest('a[href*="/watch/"]');
        if (target) {
          const config = getSiteConfig();
          if (config) {
            config.playButtonClicked = true;
            console.log('[STREAMING] Клик на play кнопку на amediateka.ru');
            // Ждем появления popup плеера и показываем виджет
            setTimeout(() => {
              checkAndShowOverlay();
            }, 2000);
          }
        }
      }, true);
    }
    
    // Отслеживание переключения серий внутри плеера (rezka/hdrezka/lordfilm/lordserial/allserial/boxserial)
    const playerTrackingHosts = hostname.includes('rezka') || hostname.includes('hdrezka') || hostname.includes('lordfilm') || hostname.includes('lordserial') || hostname.includes('allserial') || hostname.includes('boxserial');
    if (playerTrackingHosts) {
      // Селекторы контейнеров плеера для MutationObserver (один контейнер мог не ловить все сайты)
      const playerSelectors = [
        '#player-container',
        '#player',
        '.video_9Xh',
        '#player-container > iframe',
        '#allplay',
        '.controls_11s',
        '.list_5Wf',
        '.jq-selectbox__select',
        '#controls-root',
        '.player-video-bar',
        '.qsv-poster',
        '#raichuContainerWithPlayer',
        '[data-player]',
        '.video-player-wrapper',
        '.video-js',
        '.plyr'
      ];

      // Отслеживаем клик на кнопку следующей серии
      document.addEventListener('click', (e) => {
        const nextBtn = e.target.closest('.episode-next, button.episode-next, [class*="episode-next"]');
        const episodeItem = e.target.closest('.item_2mH, [class*="item"], .menu_2sa .item');
        if (nextBtn || episodeItem) {
          console.log('[STREAMING] Переключение серии в плеере');
          hasTriggeredForCurrent = false;
          setTimeout(() => {
            const info = getContentInfo();
            if (info && info.title) {
              const currentHash = getContentHash(info);
              if (currentHash !== lastContentHash) {
                console.log('[STREAMING] Обнаружено изменение серии (hash изменился):', currentHash);
                lastContentHash = currentHash;
                const key = getContentKey(info);
                lastShown[key] = 0;
                checkAndShowOverlay();
                window.dispatchEvent(new CustomEvent('movieplanner:content-detected', { detail: info }));
              }
            }
          }, 2000);
        }
      }, true);

      // MutationObserver на всех контейнерах плеера
      const playerObserver = new MutationObserver(() => {
        hasTriggeredForCurrent = false;
        const info = getContentInfo();
        if (info && info.title) {
          const currentHash = getContentHash(info);
          if (currentHash !== lastContentHash && lastContentHash !== '') {
            console.log('[STREAMING] Обнаружено изменение серии через MutationObserver');
            lastContentHash = currentHash;
            const key = getContentKey(info);
            lastShown[key] = 0;
            checkAndShowOverlay();
            window.dispatchEvent(new CustomEvent('movieplanner:content-detected', { detail: info }));
          }
        }
      });

      const observedNodes = new Set();
      playerSelectors.forEach((sel) => {
        const node = document.querySelector(sel);
        if (node && !observedNodes.has(node)) {
          observedNodes.add(node);
          try {
            playerObserver.observe(node, {
              childList: true,
              subtree: true,
              attributes: true,
              characterData: true
            });
          } catch (err) {
            // узел уже отключён или не поддерживается
          }
        }
      });

      // Video: при 80% просмотра — один показ на текущую серию
      const videoEl = document.querySelector('video');
      if (videoEl) {
        videoEl.addEventListener('timeupdate', () => {
          if (videoEl.duration <= 0) return;
          if (videoEl.currentTime >= videoEl.duration * 0.8 && !hasTriggeredForCurrent) {
            hasTriggeredForCurrent = true;
            const info = getContentInfo();
            if (info) {
              checkAndShowOverlay();
              window.dispatchEvent(new CustomEvent('movieplanner:content-detected', { detail: info }));
            }
          }
        });
      } else {
        const videoWaiter = new MutationObserver(() => {
          const v = document.querySelector('video');
          if (v) {
            videoWaiter.disconnect();
            v.addEventListener('timeupdate', () => {
              if (v.duration <= 0) return;
              if (v.currentTime >= v.duration * 0.8 && !hasTriggeredForCurrent) {
                hasTriggeredForCurrent = true;
                const info = getContentInfo();
                if (info) {
                  checkAndShowOverlay();
                  window.dispatchEvent(new CustomEvent('movieplanner:content-detected', { detail: info }));
                }
              }
            });
          }
        });
        videoWaiter.observe(document.body, { childList: true, subtree: true });
      }
    }
    
    // Первая проверка через 3 секунды после загрузки
    setTimeout(() => {
      checkAndShowOverlay();
    }, 3000);
    
    // Настраиваем наблюдение за fullscreen
    document.addEventListener('fullscreenchange', handleFullscreenChange);
    document.addEventListener('webkitfullscreenchange', handleFullscreenChange);
    document.addEventListener('mozfullscreenchange', handleFullscreenChange);
    document.addEventListener('MSFullscreenChange', handleFullscreenChange);
    
    // Настраиваем наблюдение за кликом на кнопку fullscreen
    setupFullscreenButtonObserver();
    
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
  
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.action !== 'get_streaming_page_info') return;
    if (isCatalogOrMainPage()) {
      sendResponse(null);
      return true;
    }
    const info = getContentInfo();
    sendResponse(info || null);
    return true;
  });
})();

