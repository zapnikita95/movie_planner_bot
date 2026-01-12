"""
Сервис для поиска фильмов по описанию (Шазам)
Использует TMDB датасет (оффлайн), semantic search, переводчик и whisper
"""
import os
import logging
import pandas as pd
import numpy as np
import faiss
import json
from pathlib import Path
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import torch
import gc
from tqdm import tqdm
from datetime import datetime
import whisper  # только это нужно из speech-библиотек

# В начале файла (после всех импортов)
import threading

# Глобальная блокировка для индекса
_index_lock = threading.Lock()
# Блокировка для загрузки модели
_model_lock = threading.Lock()

# Отключаем ненужный параллелизм, чтобы не было segmentation fault
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'

logger = logging.getLogger(__name__)

# Глобальные кэши моделей
_model = None
_translator = None
_whisper = None
_index = None
_movies_df = None

# Пути — относительные для локального запуска, на Railway работает так же
CACHE_DIR = Path('cache')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = Path('data/shazam')
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Кэш для huggingface моделей
os.environ['HF_HOME'] = str(CACHE_DIR / 'huggingface')
os.environ['TRANSFORMERS_CACHE'] = str(CACHE_DIR / 'huggingface' / 'transformers')
os.environ['SENTENCE_TRANSFORMERS_HOME'] = str(CACHE_DIR / 'huggingface' / 'sentence_transformers')

TMDB_CSV_PATH = CACHE_DIR / 'tmdb_movies.csv'  # 'cache/tmdb_movies.csv'
INDEX_PATH = DATA_DIR / 'tmdb_index.faiss'     # 'data/shazam/tmdb_index.faiss'
DATA_PATH = DATA_DIR / 'tmdb_movies_processed.csv'  # 'data/shazam/tmdb_movies_processed.csv'

MIN_VOTE_COUNT = 500
MAX_MOVIES = 50000


def init_shazam_index():
    """Инициализация индекса при запуске приложения"""
    logger.info("Запуск инициализации индекса шазама при старте приложения...")
    try:
        # НЕ используем блокировку здесь - get_index_and_movies() уже защищена блокировкой
        get_index_and_movies()  # Это вызовет build_tmdb_index() при необходимости
        logger.info("Индекс шазама успешно инициализирован при старте")
    except Exception as e:
        logger.error(f"Ошибка инициализации индекса при старте: {e}", exc_info=True)


def get_model():
    global _model
    # Двойная проверка с блокировкой для thread-safety
    if _model is None:
        with _model_lock:
            # Проверяем еще раз внутри блокировки
            if _model is None:
                logger.info("Загрузка модели embeddings...")
                _model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                logger.info("Модель embeddings загружена")
    return _model


def get_translator():
    global _translator
    if _translator is None:
        logger.info("Загрузка транслятора ru→en...")
        try:
            torch.set_num_threads(1)
            torch.set_grad_enabled(False)
            _translator = pipeline(
                "translation",
                model="Helsinki-NLP/opus-mt-ru-en",
                device=-1,
                torch_dtype=torch.float32
            )
            test = _translator("тестовая фраза", max_length=512)
            logger.info(f"Транслятор готов (тест: 'тестовая фраза' → '{test[0]['translation_text']}')")
        except Exception as e:
            logger.error(f"Ошибка транслятора: {e}", exc_info=True)
            _translator = False
    return _translator


def get_whisper():
    global _whisper
    if _whisper is None:
        logger.info(f"Загрузка whisper (кэш: {CACHE_DIR})...")
        try:
            whisper_cache = CACHE_DIR / 'whisper'
            whisper_cache.mkdir(parents=True, exist_ok=True)
            
            model = whisper.load_model("base", download_root=str(whisper_cache))
            
            class WhisperWrapper:
                def __init__(self, model):
                    self.model = model
                    
                def __call__(self, audio_path):
                    result = self.model.transcribe(str(audio_path), language="ru")
                    return {"text": result.get("text", "").strip()}
            
            _whisper = WhisperWrapper(model)
            logger.info("whisper успешно загружен")
        except Exception as e:
            logger.error(f"Ошибка загрузки whisper: {e}", exc_info=True)
            _whisper = False
    return _whisper


def translate_to_english(text):
    translator = get_translator()
    if not translator or translator is False:
        return text
    
    russian_chars = set('абвгдеёжзийклмнопрстуфхцчшщъыьэюя')
    if any(c.lower() in russian_chars for c in text):
        try:
            result = translator(text, max_length=512)
            return result[0]['translation_text']
        except Exception:
            return text
    return text


def transcribe_voice(audio_path):
    """Whisper — распознавание речи"""
    logger.info(f"[TRANSCRIBE] Файл: {audio_path}")
    
    whisper_model = get_whisper()
    if not whisper_model:
        logger.error("Whisper не загрузился")
        return None
        
    try:
        result = whisper_model(audio_path)
        text = result.get("text", "").strip()
        if text:
            logger.info(f"[WHISPER] Распознано: {text[:120]}...")
            return text
        logger.warning("[WHISPER] Пустой результат")
    except Exception as e:
        logger.error(f"Whisper ошибка: {e}", exc_info=True)
    
    return None


def convert_ogg_to_wav(ogg_path, wav_path, sample_rate=16000):
    """Оставляем простую конвертацию через pydub (если ещё используешь)"""
    try:
        from pydub import AudioSegment
        audio = AudioSegment.from_ogg(ogg_path)
        audio = audio.set_frame_rate(sample_rate).set_channels(1)
        audio.export(wav_path, format="wav")
        return True
    except Exception as e:
        logger.error(f"Конвертация OGG→WAV провалилась: {e}")
        return False


def parse_json_list(json_str, key='name', top_n=10):
    if pd.isna(json_str) or json_str == '[]':
        return ''
    try:
        items = json.loads(json_str)
        names = [item[key] for item in items[:top_n] if key in item]
        return ', '.join(names)
    except:
        return ''

def build_tmdb_index():
    global _index, _movies_df

    # Индекс всегда пересобирается при деплое для актуальности данных
    logger.info("Начинаем пересборку индекса TMDB...")
    
    # === СКАЧИВАНИЕ И ПОИСК CSV ФАЙЛА ===
    if not TMDB_CSV_PATH.exists():
        logger.info("TMDB CSV не найден — скачиваем через Kaggle API...")
        try:
            import kaggle
            
            kaggle_username = os.getenv("KAGGLE_USERNAME")
            kaggle_key = os.getenv("KAGGLE_KEY")
            
            if not kaggle_username or not kaggle_key:
                logger.error("KAGGLE_USERNAME и KAGGLE_KEY не установлены в переменных окружения")
                return None, None
            
            kaggle_dir = Path("/root/.kaggle")
            kaggle_dir.mkdir(parents=True, exist_ok=True)
            kaggle_json = kaggle_dir / "kaggle.json"
            
            if not kaggle_json.exists():
                with open(kaggle_json, "w") as f:
                    f.write(f'{{"username":"{kaggle_username}","key":"{kaggle_key}"}}')
                os.chmod(kaggle_json, 0o600)
                os.environ['KAGGLE_USERNAME'] = kaggle_username
                os.environ['KAGGLE_KEY'] = kaggle_key
            
            logger.info("Скачиваем датасет через Kaggle API...")
            kaggle.api.dataset_download_files(
                "alanvourch/tmdb-movies-daily-updates",
                path=str(CACHE_DIR),
                unzip=True
            )
            
            actual_csv = CACHE_DIR / "TMDB_all_movies.csv"
            if not actual_csv.exists():
                logger.error("TMDB_all_movies.csv не найден после скачивания")
                logger.info(f"Содержимое CACHE_DIR: {list(CACHE_DIR.iterdir())}")
                return None, None
            
            logger.info(f"Найден главный файл: {actual_csv.name} (размер: {actual_csv.stat().st_size / 1e6:.1f} MB)")
            
            import shutil
            shutil.copy(actual_csv, TMDB_CSV_PATH)
            logger.info(f"TMDB CSV успешно скопирован: {TMDB_CSV_PATH}")
            
        except ImportError as e:
            logger.error(f"Библиотека kaggle не установлена: {e}. Установите через: pip install kaggle", exc_info=True)
            return None, None
        except Exception as e:
            logger.error(f"Ошибка обработки TMDB датасета: {e}", exc_info=True)
            return None, None

    # === Чтение и обработка CSV ===
    logger.info("Загружаем TMDB датасет из CSV...")
    try:
        df = pd.read_csv(TMDB_CSV_PATH, low_memory=False)
        logger.info(f"Загружено {len(df)} записей")
        logger.info(f"Колонки в датасете: {', '.join(df.columns.tolist())}")
    except Exception as e:
        logger.error(f"Ошибка чтения CSV файла: {e}", exc_info=True)
        return None, None
    
    # Парсим даты (формат: 1994-06-09) для дальнейшего использования
    df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    
    # Фильтруем только обязательные поля:
    # 1. imdb_id не NaN (обязательно должен быть)
    # 2. overview и title не пустые
    logger.info(f"Фильтрация: imdb_id not NaN, overview/title not NaN")
    initial_count = len(df)
    
    # Фильтруем NaN imdb_id (важно: проверяем до преобразования в строку)
    df = df[df['imdb_id'].notna()]
    logger.info(f"После фильтра imdb_id not NaN: {len(df)} фильмов")
    
    # Также убираем строки где imdb_id после преобразования будет 'nan'
    df = df[df['imdb_id'].astype(str).str.lower() != 'nan']
    logger.info(f"После фильтра imdb_id != 'nan': {len(df)} фильмов")
    
    df = df.dropna(subset=['overview', 'title'])
    logger.info(f"После фильтра overview/title not NaN: {len(df)} фильмов")
    
    # Сортируем по популярности (vote_count, если есть) и берем топ фильмов
    if 'vote_count' in df.columns:
        df = df.sort_values('vote_count', ascending=False, na_last=True).head(MAX_MOVIES)
    else:
        df = df.head(MAX_MOVIES)
    logger.info(f"После сортировки и ограничения до {MAX_MOVIES}: {len(df)} фильмов (изначально было {initial_count})")
    
    logger.info("Keywords отсутствуют — используем только сюжет, жанры, актёров и режиссёра")
    
    df['genres_str'] = df['genres'].apply(lambda x: parse_json_list(x, 'name'))
    
    # Актёры (поле cast есть!)
    df['actors_str'] = df['cast'].apply(lambda x: parse_json_list(x, 'name', top_n=10))
    
    # Режиссёры (поле director уже готово как строка)
    df['director_str'] = df['director'].fillna('')
    
    # Продюсеры
    df['producers_str'] = df['producers'].fillna('')
    
    df['description'] = df.apply(
        lambda row: f"{row['title']} ({row['year']}) {row['genres_str']}. "
                    f"Plot: {row['overview']}. "
                    f"Actors: {row['actors_str']}. "
                    f"Director: {row['director_str']}. "
                    f"Producers: {row['producers_str']}",
        axis=1
    )
    
    # ФИКС IMDB ID — чистим .0 и убираем все tt в начале (сохраняем БЕЗ префикса tt)
    # Применяем преобразования только к валидным imdb_id (не NaN, не пустые)
    df['imdb_id'] = df['imdb_id'].astype(str).str.strip()  # убираем пробелы
    df['imdb_id'] = df['imdb_id'].str.replace(r'\.0$', '', regex=True)  # убираем .0
    # Убираем все "tt" в начале (может быть tttt или tt), сохраняем БЕЗ префикса
    df['imdb_id'] = df['imdb_id'].str.replace(r'^tt+', '', regex=True)  # убираем все tt в начале
    
    # Удаляем строки, где imdb_id стал пустым после обработки
    df = df[df['imdb_id'].str.len() > 0]
    logger.info(f"После финальной очистки imdb_id: {len(df)} фильмов")
    
    processed = df[['imdb_id', 'title', 'year', 'description']].copy()
    # Уже отсортировали и ограничили выше, не нужно еще раз .head()
    
    logger.info(f"Генерация эмбеддингов для {len(processed)} фильмов...")
    
    model = get_model()
    descriptions = processed['description'].tolist()
    
    embeddings = []
    batch_size = 32
    for i in tqdm(range(0, len(descriptions), batch_size), desc="Embeddings"):
        batch = descriptions[i:i+batch_size]
        batch_emb = model.encode(batch, show_progress_bar=False)
        embeddings.extend(batch_emb)
    
    embeddings = np.array(embeddings).astype('float32')
    dimension = embeddings.shape[1]
    
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings)
    
    faiss.write_index(index, str(INDEX_PATH))
    processed.to_csv(DATA_PATH, index=False)
    
    _index = index
    _movies_df = processed
    
    logger.info(f"Готово! Создан индекс на {len(processed)} фильмов")
    return index, processed

def get_index_and_movies():
    global _index, _movies_df
    
    logger.info("[GET INDEX] Проверка состояния индекса...")
    
    # Сначала проверяем без блокировки, если индекс уже загружен
    if _index is not None and _movies_df is not None:
        logger.info(f"[GET INDEX] Индекс уже загружен в памяти, фильмов: {len(_movies_df)}")
        return _index, _movies_df
    
    logger.info("[GET INDEX] Индекс не в памяти, пытаемся загрузить...")
    
    with _index_lock:  # ← Только один worker может войти сюда одновременно
        logger.info("[GET INDEX] Получена блокировка для загрузки индекса...")
        # Двойная проверка - возможно, другой поток уже загрузил индекс
        if _index is not None and _movies_df is not None:
            logger.info(f"[GET INDEX] Индекс уже загружен другим потоком, фильмов: {len(_movies_df)}")
            return _index, _movies_df
        
        logger.info("[GET INDEX] Загружаем индекс через build_tmdb_index()...")
        try:
            _index, _movies_df = build_tmdb_index()
            if _index is not None and _movies_df is not None:
                logger.info(f"[GET INDEX] Индекс успешно загружен, фильмов: {len(_movies_df)}")
            else:
                logger.warning("[GET INDEX] build_tmdb_index() вернул None")
            return _index, _movies_df
        except Exception as e:
            logger.error(f"[GET INDEX] Ошибка при загрузке индекса: {e}", exc_info=True)
            return None, None

def search_movies(query, top_k=15):
    try:
        logger.info(f"[SEARCH MOVIES] Начало поиска для запроса: '{query}'")
        
        logger.info(f"[SEARCH MOVIES] Шаг 1: Перевод запроса...")
        query_en = translate_to_english(query)
        logger.info(f"[SEARCH MOVIES] Переведено: '{query}' → '{query_en}'")
        
        logger.info(f"[SEARCH MOVIES] Шаг 2: Получение индекса и данных...")
        index, movies = get_index_and_movies()
        if index is None:
            logger.warning("[SEARCH MOVIES] Индекс не найден, возвращаем пустой список")
            return []
        logger.info(f"[SEARCH MOVIES] Индекс получен, фильмов: {len(movies)}")
        
        logger.info(f"[SEARCH MOVIES] Шаг 3: Получение модели embeddings...")
        model = get_model()
        logger.info(f"[SEARCH MOVIES] Модель получена")
        
        logger.info(f"[SEARCH MOVIES] Шаг 4: Создание эмбеддинга запроса...")
        query_emb = model.encode([query_en])[0].astype('float32').reshape(1, -1)
        logger.info(f"[SEARCH MOVIES] Эмбеддинг создан, размер: {query_emb.shape}")
        
        logger.info(f"[SEARCH MOVIES] Шаг 5: Поиск в индексе (top_k={top_k})...")
        D, I = index.search(query_emb, k=top_k)
        logger.info(f"[SEARCH MOVIES] Поиск завершен, найдено индексов: {len(I[0])}")
        
        logger.info(f"[SEARCH MOVIES] Шаг 6: Формирование результатов...")
        results = []
        for idx in I[0]:
            if idx < len(movies):
                row = movies.iloc[idx]
                imdb_id_raw = str(row['imdb_id']).strip()
                
                # Очистка IMDB ID: убираем .0, убираем все tt в начале, добавляем один tt
                imdb_id_clean = imdb_id_raw.replace('.0', '').replace('.', '')  # Убираем .0 и другие точки
                imdb_id_clean = imdb_id_clean.lstrip('t')  # Убираем все tt в начале
                if imdb_id_clean and imdb_id_clean.isdigit():
                    imdb_id_clean = f"tt{imdb_id_clean}"
                else:
                    imdb_id_clean = imdb_id_raw  # Если не получилось очистить, оставляем как есть
                
                if imdb_id_clean != imdb_id_raw:
                    logger.info(f"[SEARCH MOVIES] ID преобразован: '{imdb_id_raw}' → '{imdb_id_clean}'")
                
                results.append({
                    'imdb_id': imdb_id_clean,
                    'title': row['title'],
                    'year': row['year'] if pd.notna(row['year']) else None,
                    'description': row['description'][:500]
                })
        logger.info(f"[SEARCH MOVIES] Результаты сформированы, найдено: {len(results)} фильмов")
        return results
    except Exception as e:
        logger.error(f"[SEARCH MOVIES] Ошибка поиска фильмов: {e}", exc_info=True)
        return []