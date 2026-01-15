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
MAX_MOVIES = 20000


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
                # Позволяем выбрать модель через переменную окружения для оптимизации на Railway
                model_name = os.getenv('EMBEDDINGS_MODEL', 'BAAI/bge-large-en-v1.5')
                # Если установлен USE_FAST_EMBEDDINGS=1, используем более легкую модель
                if os.getenv('USE_FAST_EMBEDDINGS', '0').strip().lower() in ('1', 'true', 'yes', 'on'):
                    model_name = 'BAAI/bge-base-en-v1.5'
                    logger.info("⚠️ USE_FAST_EMBEDDINGS=1 — используем более легкую модель для ускорения")
                _model = SentenceTransformer(model_name)
                logger.info(f"Модель embeddings загружена ({model_name} — лучшая для retrieval на английском)")
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
                model="facebook/nllb-200-distilled-600M",
                src_lang="rus_Cyrl",
                tgt_lang="eng_Latn",
                device=-1,
                torch_dtype=torch.float32
            )
            test = _translator("тестовая фраза", max_length=512)
            logger.info(f"Транслятор готов (тест: 'тестовая фраза' → '{test[0]['translation_text']}')")
            logger.info("Транслятор загружен (nllb-200-distilled-600M — лучше для контекста и исторических терминов)")
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
            translated = result[0]['translation_text']
            
            # Фикс для "Великая депрессия"
            if "great depression" in translated.lower():
                translated = translated.replace("great depression", "Great Depression")
                translated = translated.replace("Great depression", "Great Depression")
                translated = translated.replace("great Depression", "Great Depression")
            
            return translated
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

    # Проверяем переменную окружения для принудительной пересборки
    force_rebuild = os.getenv('FORCE_REBUILD_INDEX', '0').strip().lower() in ('1', 'true', 'yes', 'on')
    if force_rebuild:
        logger.warning("⚠️ FORCE_REBUILD_INDEX=1 - принудительная пересборка индекса!")
        # Удаляем существующий индекс и данные
        try:
            if INDEX_PATH.exists():
                INDEX_PATH.unlink()
                logger.info("Удален существующий индекс для пересборки")
            if DATA_PATH.exists():
                DATA_PATH.unlink()
                logger.info("Удалены существующие данные для пересборки")
        except Exception as e:
            logger.warning(f"Ошибка при удалении старого индекса: {e}")
    
    # Проверяем, существует ли индекс - если да, загружаем его вместо пересборки
    if not force_rebuild and INDEX_PATH.exists() and DATA_PATH.exists():
        logger.info(f"Индекс уже существует ({INDEX_PATH}), загружаем из файла...")
        try:
            _index = faiss.read_index(str(INDEX_PATH))
            _movies_df = pd.read_csv(DATA_PATH)
            
            # КРИТИЧНО: Проверяем совпадение размерности индекса и текущей модели
            model = get_model()
            expected_dim = model.get_sentence_embedding_dimension()
            actual_dim = _index.d
            
            if expected_dim != actual_dim:
                logger.warning(f"Размерность индекса ({actual_dim}) не совпадает с размерностью модели ({expected_dim})!")
                logger.warning(f"Индекс был построен с другой моделью. Пересобираем индекс...")
                _index = None
                _movies_df = None
                # Удаляем старый индекс, чтобы пересобрать
                try:
                    INDEX_PATH.unlink()
                    DATA_PATH.unlink()
                    logger.info("Старый индекс удален для пересборки")
                except Exception as e:
                    logger.warning(f"Не удалось удалить старый индекс: {e}")
            else:
                # Проверяем наличие actors_str и director_str в загруженном DataFrame
                has_actors = 'actors_str' in _movies_df.columns
                has_director = 'director_str' in _movies_df.columns
                if not has_actors or not has_director:
                    logger.warning(f"Индекс не содержит actors_str или director_str (has_actors={has_actors}, has_director={has_director})")
                    logger.warning("Для максимальной эффективности keyword-матчинга рекомендуется пересобрать индекс с FORCE_REBUILD_INDEX=1")
                logger.info(f"Индекс успешно загружен из файла, фильмов: {len(_movies_df)}, размерность: {actual_dim}")
                return _index, _movies_df
        except Exception as e:
            logger.warning(f"Ошибка загрузки существующего индекса: {e}, пересобираем...", exc_info=True)
    
    # Индекс не существует или не загрузился - пересобираем
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
        # Пробуем разные способы чтения CSV для обработки проблемных строк
        import inspect
        sig = inspect.signature(pd.read_csv)
        
        df = None
        error = None
        
        # Попытка 1: Современный pandas (>= 1.3.0) с on_bad_lines
        if 'on_bad_lines' in sig.parameters:
            try:
                df = pd.read_csv(
                    TMDB_CSV_PATH, 
                    low_memory=False,
                    on_bad_lines='skip',  # Пропускаем проблемные строки
                    encoding='utf-8'
                )
                logger.info(f"✅ Загружено {len(df)} записей (с on_bad_lines='skip')")
            except Exception as e1:
                error = e1
                logger.warning(f"Попытка 1 не удалась: {e1}")
        
        # Попытка 2: Python engine (более гибкий парсер)
        # ВАЖНО: Python engine не поддерживает low_memory параметр
        if df is None:
            try:
                kwargs = {
                    'engine': 'python',
                    'encoding': 'utf-8'
                }
                # Добавляем параметр для обработки проблемных строк
                sig = inspect.signature(pd.read_csv)
                if 'on_bad_lines' in sig.parameters:
                    kwargs['on_bad_lines'] = 'skip'
                elif 'error_bad_lines' in sig.parameters:
                    kwargs['error_bad_lines'] = False
                
                df = pd.read_csv(TMDB_CSV_PATH, **kwargs)
                logger.info(f"✅ Загружено {len(df)} записей (через Python engine)")
            except Exception as e2:
                error = e2
                logger.warning(f"Попытка 2 не удалась: {e2}")
        
        # Попытка 3: С явными параметрами для кавычек
        # ВАЖНО: Python engine не поддерживает low_memory параметр
        if df is None:
            try:
                kwargs = {
                    'engine': 'python',
                    'encoding': 'utf-8',
                    'quotechar': '"',
                    'escapechar': '\\',
                    'doublequote': True
                }
                sig = inspect.signature(pd.read_csv)
                if 'on_bad_lines' in sig.parameters:
                    kwargs['on_bad_lines'] = 'skip'
                elif 'error_bad_lines' in sig.parameters:
                    kwargs['error_bad_lines'] = False
                
                df = pd.read_csv(TMDB_CSV_PATH, **kwargs)
                logger.info(f"✅ Загружено {len(df)} записей (с явными параметрами кавычек)")
            except Exception as e3:
                error = e3
                logger.warning(f"Попытка 3 не удалась: {e3}")
        
        # Попытка 4: Чтение по частям (chunksize) для обхода проблемных строк
        if df is None:
            try:
                logger.info("Попытка 4: Чтение файла по частям для обхода проблемных строк...")
                chunks = []
                chunk_size = 10000
                skipped_rows = 0
                
                sig = inspect.signature(pd.read_csv)
                kwargs = {
                    'engine': 'python',
                    'encoding': 'utf-8',
                    'chunksize': chunk_size
                }
                if 'on_bad_lines' in sig.parameters:
                    kwargs['on_bad_lines'] = 'skip'
                elif 'error_bad_lines' in sig.parameters:
                    kwargs['error_bad_lines'] = False
                
                for chunk in pd.read_csv(TMDB_CSV_PATH, **kwargs):
                    chunks.append(chunk)
                
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                    logger.info(f"✅ Загружено {len(df)} записей (по частям, пропущено проблемных строк: {skipped_rows})")
                else:
                    raise Exception("Не удалось загрузить ни одного чанка")
            except Exception as e4:
                error = e4
                logger.warning(f"Попытка 4 не удалась: {e4}")
        
        if df is None:
            raise Exception(f"Не удалось загрузить CSV после всех попыток. Последняя ошибка: {error}")
        
        logger.info(f"✅ Успешно загружено {len(df)} записей")
        logger.info(f"Колонки в датасете: {', '.join(df.columns.tolist())}")
    except Exception as e:
        logger.error(f"Ошибка чтения CSV файла после всех попыток: {e}", exc_info=True)
        return None, None
    
    # Парсим даты (формат: 1994-06-09) для дальнейшего использования
    df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    
    # Фильтруем только обязательные поля:
    # 1. imdb_id не NaN (обязательно должен быть)
    # 2. title ИЛИ original_title не пустые (хотя бы одно должно быть)
    # 3. overview МОЖЕТ быть пустым (но будет учитываться при приоритизации поиска)
    logger.info(f"Фильтрация: imdb_id not NaN, (title OR original_title) not NaN")
    initial_count = len(df)
    
    # Фильтруем NaN imdb_id (важно: проверяем до преобразования в строку)
    df = df[df['imdb_id'].notna()]
    logger.info(f"После фильтра imdb_id not NaN: {len(df)} фильмов")
    
    # Также убираем строки где imdb_id после преобразования будет 'nan'
    df = df[df['imdb_id'].astype(str).str.lower() != 'nan']
    logger.info(f"После фильтра imdb_id != 'nan': {len(df)} фильмов")
    
    # Фильтр: title ИЛИ original_title не пустые
    df = df[df['title'].notna() | df['original_title'].notna()]
    logger.info(f"После фильтра (title OR original_title) not NaN: {len(df)} фильмов")
    
    # Убираем пустые title (но original_title может остаться)
    df = df[(df['title'].notna() & (df['title'].astype(str).str.strip() != '')) | 
            (df['original_title'].notna() & (df['original_title'].astype(str).str.strip() != ''))]
    logger.info(f"После фильтра (title OR original_title) not empty: {len(df)} фильмов")
    
    # Фильтруем по минимальному количеству голосов
    if 'vote_count' in df.columns:
        df = df[df['vote_count'] >= MIN_VOTE_COUNT]
        logger.info(f"После фильтра vote_count >= {MIN_VOTE_COUNT}: {len(df)} фильмов")
    
    # Сортируем по популярности (vote_count, если есть) и берем топ фильмов
    # NaN значения по умолчанию идут в конец при ascending=False
    if 'vote_count' in df.columns:
        df = df.sort_values('vote_count', ascending=False).head(MAX_MOVIES)
    else:
        df = df.head(MAX_MOVIES)
    logger.info(f"После сортировки и ограничения до {MAX_MOVIES}: {len(df)} фильмов (изначально было {initial_count})")
    
    logger.info("Keywords отсутствуют — используем только сюжет, жанры, актёров, режиссёра и страны производства")
    
    df['genres_str'] = df['genres'].apply(lambda x: parse_json_list(x, 'name'))
    
    # Актёры (поле cast есть!)
    df['actors_str'] = df['cast'].apply(lambda x: parse_json_list(x, 'name', top_n=10))
    
    # Режиссёры (поле director уже готово как строка)
    df['director_str'] = df['director'].fillna('')
    
    # Извлекаем всех актёров и режиссёров для построения топ-списков
    logger.info("Извлечение всех актёров и режиссёров для построения топ-списков...")
    from collections import Counter
    
    all_actors = []
    all_directors = []
    
    for idx, row in df.iterrows():
        # Актёры из cast
        if pd.notna(row.get('cast')):
            actors_list = parse_json_list(row['cast'], 'name', top_n=None)  # Все актёры
            if actors_list:
                all_actors.extend([actor.lower().strip() for actor in actors_list])
        
        # Режиссёры
        if pd.notna(row.get('director')) and str(row['director']).strip():
            director = str(row['director']).strip().lower()
            if director and director != 'nan':
                all_directors.append(director)
    
    # Подсчитываем частоту появления
    actor_counts = Counter(all_actors)
    director_counts = Counter(all_directors)
    
    # Топ-500 актёров и топ-100 режиссёров
    top_500_actors = [actor for actor, count in actor_counts.most_common(500)]
    top_100_directors = [director for director, count in director_counts.most_common(100)]
    
    # Сохраняем в файлы
    TOP_ACTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TOP_ACTORS_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(top_500_actors))
    logger.info(f"✅ Сохранён топ-500 актёров: {len(top_500_actors)} имён (файл: {TOP_ACTORS_PATH})")
    
    with open(TOP_DIRECTORS_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(top_100_directors))
    logger.info(f"✅ Сохранён топ-100 режиссёров: {len(top_100_directors)} имён (файл: {TOP_DIRECTORS_PATH})")
    
    # Продюсеры
    df['producers_str'] = df['producers'].fillna('')
    
    # Страны производства
    df['countries_str'] = df['production_countries'].apply(lambda x: parse_json_list(x, 'name'))
    
    # Сохраняем информацию о наличии overview для приоритизации при поиске
    df['has_overview'] = df['overview'].notna() & (df['overview'].astype(str).str.strip() != '')
    
    # Используем title, если есть, иначе original_title
    df['display_title'] = df['title'].fillna(df['original_title'])
    
    df['description'] = df.apply(
        lambda row: f"{row['display_title']} ({row['year']}) {row['genres_str']}. "
                    f"{('Plot: ' + str(row['overview']) + '. ') if row.get('has_overview', False) else ''}"
                    f"Actors: {row['actors_str']}. "
                    f"Director: {row['director_str']}. "
                    f"Producers: {row['producers_str']}. "
                    f"Countries: {row['countries_str']}",
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
    
    # Сохраняем has_overview, actors_str, director_str, genres_str, overview и vote_count для приоритизации и keyword-матчинга при поиске
    # overview сохраняем отдельно для keyword-матчинга (самый сильный буст)
    # vote_count нужен для буста по популярности
    columns_to_save = ['imdb_id', 'title', 'year', 'description', 'has_overview', 'actors_str', 'director_str', 'genres_str', 'overview']
    if 'vote_count' in df.columns:
        columns_to_save.append('vote_count')
    processed = df[columns_to_save].copy()
    # Заменяем NaN на пустые строки для безопасной работы
    processed['overview'] = processed['overview'].fillna('')
    processed['genres_str'] = processed['genres_str'].fillna('')
    # Уже отсортировали и ограничили выше, не нужно еще раз .head()
    
    # КЭШИРОВАНИЕ: Проверяем, не были ли эмбеддинги уже сгенерированы
    # Если индекс существует, значит эмбеддинги уже вычислены и сохранены
    if INDEX_PATH.exists() and DATA_PATH.exists():
        logger.info(f"✅ Индекс уже существует ({INDEX_PATH}) - эмбеддинги уже сгенерированы и сохранены")
        logger.info("Загружаем индекс из файла вместо перегенерации эмбеддингов...")
        try:
            _index = faiss.read_index(str(INDEX_PATH))
            _movies_df = pd.read_csv(DATA_PATH)
            
            # Проверяем совпадение размерности с текущей моделью
            model = get_model()
            expected_dim = model.get_sentence_embedding_dimension()
            actual_dim = _index.d
            
            if expected_dim != actual_dim:
                logger.warning(f"Размерность индекса ({actual_dim}) не совпадает с моделью ({expected_dim}) - пересобираем")
                _index = None
                _movies_df = None
            else:
                logger.info(f"✅ Индекс загружен из кэша, фильмов: {len(_movies_df)}, размерность: {actual_dim}")
                return _index, _movies_df
        except Exception as e:
            logger.warning(f"Ошибка загрузки кэшированного индекса: {e}, пересобираем...", exc_info=True)
    
    # Генерируем эмбеддинги только если индекс не существует или не загрузился
    logger.info(f"Генерация эмбеддингов для {len(processed)} фильмов...")
    logger.info("⚠️ Это займет несколько минут. Эмбеддинги будут сохранены в кэш для следующего запуска.")
    
    model = get_model()
    descriptions = processed['description'].tolist()
    
    # Оптимизация: увеличиваем batch_size для ускорения (можно настроить через переменную окружения)
    # Для Railway рекомендуется 64-128 (зависит от доступной памяти)
    # Для локальной машины с GPU можно 256-512
    batch_size = int(os.getenv('EMBEDDINGS_BATCH_SIZE', '64'))
    logger.info(f"Используется batch_size={batch_size} для генерации эмбеддингов")
    logger.info(f"💡 Совет: для максимального ускорения на Railway установите USE_FAST_EMBEDDINGS=1 и EMBEDDINGS_BATCH_SIZE=128")
    
    embeddings = []
    total_batches = (len(descriptions) + batch_size - 1) // batch_size
    logger.info(f"Всего батчей для обработки: {total_batches}")
    
    for i in tqdm(range(0, len(descriptions), batch_size), desc="Embeddings", total=total_batches):
        batch = descriptions[i:i+batch_size]
        # Оптимизации для ускорения: convert_to_numpy=True, normalize_embeddings=False
        batch_emb = model.encode(
            batch, 
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=False,
            batch_size=batch_size
        )
        embeddings.extend(batch_emb)
        
        # Периодически логируем прогресс для мониторинга
        if (i // batch_size + 1) % 10 == 0:
            logger.info(f"Обработано {i + len(batch)}/{len(descriptions)} фильмов ({(i + len(batch)) / len(descriptions) * 100:.1f}%)")
    
    embeddings = np.array(embeddings).astype('float32')
    dimension = embeddings.shape[1]
    
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings)
    
    # Сохраняем индекс в кэш для следующего запуска
    logger.info(f"Сохранение индекса в кэш: {INDEX_PATH}")
    faiss.write_index(index, str(INDEX_PATH))
    processed.to_csv(DATA_PATH, index=False)
    logger.info("✅ Индекс и эмбеддинги сохранены в кэш")
    
    _index = index
    _movies_df = processed
    
    logger.info(f"Готово! Создан индекс на {len(processed)} фильмов")
    return index, processed

def load_top_actors_and_directors():
    """Загружает топ-500 актёров и топ-100 режиссёров из файлов"""
    global _top_actors_set, _top_directors_set
    
    if _top_actors_set is not None and _top_directors_set is not None:
        return _top_actors_set, _top_directors_set
    
    _top_actors_set = set()
    _top_directors_set = set()
    
    # Загружаем топ-500 актёров
    if TOP_ACTORS_PATH.exists():
        try:
            with open(TOP_ACTORS_PATH, 'r', encoding='utf-8') as f:
                _top_actors_set = {line.strip().lower() for line in f if line.strip()}
            logger.info(f"✅ Загружено {len(_top_actors_set)} актёров из топ-500")
        except Exception as e:
            logger.warning(f"Ошибка загрузки топ-актёров: {e}")
            _top_actors_set = set()
    else:
        logger.warning(f"Файл топ-актёров не найден: {TOP_ACTORS_PATH}")
    
    # Загружаем топ-100 режиссёров
    if TOP_DIRECTORS_PATH.exists():
        try:
            with open(TOP_DIRECTORS_PATH, 'r', encoding='utf-8') as f:
                _top_directors_set = {line.strip().lower() for line in f if line.strip()}
            logger.info(f"✅ Загружено {len(_top_directors_set)} режиссёров из топ-100")
        except Exception as e:
            logger.warning(f"Ошибка загрузки топ-режиссёров: {e}")
            _top_directors_set = set()
    else:
        logger.warning(f"Файл топ-режиссёров не найден: {TOP_DIRECTORS_PATH}")
    
    return _top_actors_set, _top_directors_set


def get_index_and_movies():
    global _index, _movies_df
    
    logger.info("[GET INDEX] Проверка состояния индекса...")
    
    # Проверяем переменную окружения для принудительной пересборки
    force_rebuild = os.getenv('FORCE_REBUILD_INDEX', '0').strip().lower() in ('1', 'true', 'yes', 'on')
    if force_rebuild:
        logger.warning("[GET INDEX] ⚠️ FORCE_REBUILD_INDEX=1 - принудительная пересборка индекса!")
        # Удаляем существующий индекс и данные
        try:
            if INDEX_PATH.exists():
                INDEX_PATH.unlink()
                logger.info("[GET INDEX] Удален существующий индекс")
            if DATA_PATH.exists():
                DATA_PATH.unlink()
                logger.info("[GET INDEX] Удалены существующие данные")
        except Exception as e:
            logger.warning(f"[GET INDEX] Ошибка при удалении старого индекса: {e}")
        # Сбрасываем глобальные переменные
        _index = None
        _movies_df = None
    
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
                # Загружаем топ-списки актёров и режиссёров
                load_top_actors_and_directors()
            else:
                logger.warning("[GET INDEX] build_tmdb_index() вернул None")
            return _index, _movies_df
        except Exception as e:
            logger.error(f"[GET INDEX] Ошибка при загрузке индекса: {e}", exc_info=True)
            return None, None

def _normalize_text(text):
    """Нормализует текст: приводит к нижнему регистру и убирает знаки препинания"""
    import re
    # Убираем знаки препинания, оставляем только буквы и цифры, приводим к нижнему регистру
    normalized = re.sub(r'[^\w\s]', '', str(text).lower())
    return normalized


def _get_genre_keywords():
    """Возвращает облака смыслов для жанров (характерные слова на английском)"""
    return {
        'action': [
            'shootout', 'chase', 'fight', 'danger', 'killer', 'villain', 'explosion', 'gun', 'weapon', 'battle',
            'combat', 'war', 'soldier', 'spy', 'agent', 'mission', 'rescue', 'escape', 'pursuit', 'conflict',
            'violence', 'action', 'thriller', 'adrenaline', 'stunt', 'hero', 'enemy', 'attack', 'defense', 'survival'
        ],
        'comedy': [
            'funny', 'laugh', 'humor', 'joke', 'comic', 'hilarious', 'amusing', 'entertaining', 'light', 'cheerful',
            'silly', 'witty', 'satire', 'parody', 'romantic comedy', 'slapstick', 'absurd', 'quirky', 'playful',
            'humorous', 'comedy', 'fun', 'gag', 'prank', 'mischief', 'comical', 'laughable', 'ridiculous', 'wacky'
        ],
        'thriller': [
            'suspense', 'tension', 'mystery', 'intrigue', 'plot', 'twist', 'surprise', 'suspicious', 'dangerous',
            'threatening', 'fear', 'anxiety', 'nervous', 'edge', 'cliffhanger', 'unpredictable', 'shocking',
            'disturbing', 'psychological', 'thriller', 'suspenseful', 'nail-biting', 'gripping', 'intense',
            'chilling', 'terrifying', 'ominous', 'sinister', 'menacing', 'alarming'
        ],
        'drama': [
            'emotional', 'serious', 'tragic', 'melodrama', 'conflict', 'struggle', 'relationship', 'family',
            'love', 'loss', 'grief', 'sorrow', 'pain', 'suffering', 'human', 'realistic', 'deep', 'meaningful',
            'touching', 'heartfelt', 'dramatic', 'intense', 'powerful', 'moving', 'profound', 'thoughtful',
            'contemplative', 'reflective', 'poignant', 'heartbreaking'
        ],
        'horror': [
            'scary', 'frightening', 'terrifying', 'horror', 'monster', 'ghost', 'demon', 'zombie', 'vampire',
            'killer', 'murder', 'death', 'blood', 'gore', 'nightmare', 'fear', 'terror', 'panic', 'dread',
            'creepy', 'spooky', 'eerie', 'sinister', 'dark', 'evil', 'supernatural', 'paranormal', 'haunted',
            'disturbing', 'shocking', 'gruesome'
        ],
        'romance': [
            'love', 'romance', 'romantic', 'relationship', 'couple', 'dating', 'wedding', 'marriage', 'kiss',
            'passion', 'affection', 'heart', 'soulmate', 'sweet', 'tender', 'intimate', 'emotional', 'caring',
            'devoted', 'loving', 'adoring', 'charming', 'enchanting', 'beautiful', 'dreamy', 'sentimental',
            'touching', 'heartwarming', 'endearing', 'affectionate'
        ],
        'animation': [
            'cartoon', 'animated', 'animation', 'drawing', 'illustration', 'picture', 'graphic', 'visual',
            'artistic', 'creative', 'colorful', 'vibrant', 'fantasy', 'imaginative', 'whimsical', 'playful',
            'childlike', 'innocent', 'magical', 'enchanting', 'fairy tale', 'storybook', 'pixar', 'disney',
            'family', 'children', 'kids', 'youthful', 'cheerful', 'bright', 'lively'
        ],
        'crime': [
            'crime', 'criminal', 'gangster', 'mafia', 'police', 'detective', 'investigation', 'murder', 'killing',
            'robbery', 'theft', 'corruption', 'illegal', 'law', 'justice', 'prison', 'criminal', 'felony',
            'violence', 'danger', 'suspense', 'mystery', 'thriller', 'underworld', 'organized crime', 'heist',
            'conspiracy', 'betrayal', 'revenge', 'punishment'
        ],
        'sci-fi': [
            'science fiction', 'sci-fi', 'future', 'space', 'alien', 'robot', 'technology', 'advanced', 'scientific',
            'futuristic', 'spacecraft', 'planet', 'galaxy', 'universe', 'time travel', 'dystopia', 'utopia',
            'cyberpunk', 'artificial intelligence', 'genetic', 'experiment', 'discovery', 'innovation', 'virtual',
            'digital', 'quantum', 'dimension', 'parallel', 'extraterrestrial', 'cosmic'
        ],
        'adventure': [
            'adventure', 'journey', 'quest', 'expedition', 'exploration', 'discovery', 'treasure', 'hunt',
            'travel', 'voyage', 'expedition', 'explorer', 'hero', 'brave', 'courageous', 'daring', 'bold',
            'exciting', 'thrilling', 'action', 'danger', 'risk', 'challenge', 'mission', 'goal', 'destination',
            'unknown', 'mysterious', 'exotic', 'foreign'
        ]
    }


def _detect_genre_from_keywords(keywords, query_en_lower):
    """Определяет жанр на основе ключевых слов и облаков смыслов"""
    genre_keywords_map = _get_genre_keywords()
    detected_genres = []
    
    # Проверяем каждое ключевое слово на принадлежность к жанрам
    for genre, genre_words in genre_keywords_map.items():
        matches = sum(1 for word in keywords if word in genre_words)
        # Также проверяем весь запрос на наличие характерных слов
        query_matches = sum(1 for word in genre_words if word in query_en_lower)
        total_matches = matches + query_matches
        
        if total_matches >= 2:  # Если найдено 2+ совпадения - жанр обнаружен
            detected_genres.append(genre)
            logger.info(f"[SEARCH MOVIES] Обнаружен жанр '{genre}' по ключевым словам (совпадений: {total_matches})")
    
    return detected_genres

def _extract_keywords(query_en):
    """Извлекает ключевые слова из запроса, убирая стоп-слова"""
    # Стоп-слова на английском (игнорируем при keyword-матчинге)
    stop_words = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
        'from', 'up', 'about', 'into', 'through', 'during', 'including', 'against', 'among',
        'throughout', 'despite', 'towards', 'upon', 'concerning', 'to', 'of', 'in', 'for',
        'film', 'movie', 'films', 'movies', 'plays', 'playing', 'actor', 'actors', 'director',
        'directors', 'starring', 'star', 'stars', 'cast', 'about', 'with', 'in', 'a', 'the'
    }
    
    # Нормализуем запрос (убираем пунктуацию, приводим к нижнему регистру)
    normalized_query = _normalize_text(query_en)
    
    # Разбиваем на слова
    import re
    words = re.findall(r'\b\w+\b', normalized_query)
    # Фильтруем стоп-слова и короткие слова (меньше 2 символов)
    keywords = [w for w in words if w not in stop_words and len(w) > 2]
    logger.info(f"[SEARCH MOVIES] Извлечены ключевые слова из '{query_en}': {keywords}")
    return keywords


def search_movies(query, top_k=15):
    try:
        logger.info(f"[SEARCH MOVIES] Начало поиска для запроса: '{query}'")
        
        logger.info(f"[SEARCH MOVIES] Шаг 1: Перевод запроса...")
        query_en = translate_to_english(query)
        logger.info(f"[SEARCH MOVIES] Переведено: '{query}' → '{query_en}'")
        
        # Извлекаем ключевые слова для обычного keyword-матчинга
        keywords = _extract_keywords(query_en)
        query_en_lower = query_en.lower()
        
        # Пытаемся извлечь имя актёра/режиссёра из запроса
        # Сначала ищем известные имена в переведённом запросе
        mentioned_actor_en = None
        known_full_names = [
            'keanu reeves',
            'leonardo dicaprio',
            'paul thomas anderson',
            'quentin tarantino',
            'martin scorsese',
            'christopher nolan',
            'david fincher',
            'ridley scott',
            'steven spielberg',
            'james cameron',
        ]
        for name in known_full_names:
            if name in query_en_lower:
                mentioned_actor_en = name
                logger.info(f"[SEARCH MOVIES] Найдено известное имя в запросе: '{mentioned_actor_en}'")
                break
        
        # Если не нашли известное имя, но запрос очень короткий (2-3 слова) - возможно это имя
        if not mentioned_actor_en:
            query_en_words = query_en.split()
            if len(query_en_words) <= 3:
                # Очень короткий запрос - возможно это имя актёра
                mentioned_actor_en = query_en.lower().strip()
                logger.info(f"[SEARCH MOVIES] Очень короткий запрос, предполагаем что это имя: '{mentioned_actor_en}'")
        
        logger.info(f"[SEARCH MOVIES] Упомянут актёр? {bool(mentioned_actor_en)}, имя (en): {mentioned_actor_en}")
        
        # Определяем жанры на основе ключевых слов и облаков смыслов
        detected_genres = _detect_genre_from_keywords(keywords, query_en_lower)
        logger.info(f"[SEARCH MOVIES] Обнаружены жанры по ключевым словам: {detected_genres}")
        
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
        
        logger.info(f"[SEARCH MOVIES] Шаг 5: Поиск в индексе...")
        
        query_dim = query_emb.shape[1]
        index_dim = index.d
        if query_dim != index_dim:
            logger.error(f"[SEARCH MOVIES] КРИТИЧЕСКАЯ ОШИБКА: Размерность запроса ({query_dim}) не совпадает с индексом ({index_dim})!")
            return []
        
        # Всегда делаем FAISS поиск (получаем больше кандидатов)
        D, I = index.search(query_emb, k=top_k * 5)
        logger.info(f"[SEARCH MOVIES] Поиск завершен, найдено индексов: {len(I[0])}")
        
        # Если актёр/режиссёр упомянут — фильтруем только его фильмы
        # ПРИОРИТЕТ: сначала актёры, потом режиссёры
        candidate_indices = []
        candidate_distances = []
        if mentioned_actor_en:
            # Нормализуем имя для поиска (убираем пунктуацию, приводим к нижнему регистру)
            actor_name_for_search = _normalize_text(mentioned_actor_en)
            
            # Проверяем, является ли имя актёром или режиссёром из топ-списков
            is_actor = actor_name_for_search in top_actors_set
            is_director = actor_name_for_search in top_directors_set
            
            # Сначала собираем фильмы с актёром (приоритет №1)
            actor_indices = []
            actor_distances = []
            # Затем собираем фильмы с режиссёром (приоритет №2, только если нет актёров)
            director_indices = []
            director_distances = []
            
            for i, idx in enumerate(I[0]):
                if idx < len(movies):
                    row = movies.iloc[idx]
                    # Нормализуем тексты из базы
                    actors_normalized = _normalize_text(row.get('actors_str', '')) if 'actors_str' in row.index else ''
                    director_normalized = _normalize_text(row.get('director_str', '')) if 'director_str' in row.index else ''
                    
                    # ПРИОРИТЕТ №1: Проверяем актёров (если имя в топ-актёрах или просто проверяем)
                    if is_actor and actor_name_for_search in actors_normalized:
                        actor_indices.append(idx)
                        actor_distances.append(float(D[0][i]))
                    # ПРИОРИТЕТ №2: Проверяем режиссёров (только если не нашли в актёрах)
                    elif (is_director or not is_actor) and actor_name_for_search in director_normalized:
                        director_indices.append(idx)
                        director_distances.append(float(D[0][i]))
            
            # Сначала добавляем фильмы с актёром, потом с режиссёром
            candidate_indices = actor_indices + director_indices
            candidate_distances = actor_distances + director_distances
            
            logger.info(f"[SEARCH MOVIES] Найдено фильмов с актёром '{actor_name_for_search}': {len(actor_indices)}, с режиссёром: {len(director_indices)}")
        else:
            # Обычный поиск - берём все результаты
            candidate_indices = I[0].tolist()
            candidate_distances = [float(D[0][i]) for i in range(len(I[0]))]
            logger.info(f"[SEARCH MOVIES] Обычный поиск, кандидатов: {len(candidate_indices)}")
        
        # Ранжируем кандидатов
        logger.info(f"[SEARCH MOVIES] Шаг 6: Формирование результатов...")
        results = []
        for i, idx in enumerate(candidate_indices):
            if idx >= len(movies):
                continue
            row = movies.iloc[idx]
            imdb_id_raw = str(row['imdb_id']).strip()
            
            imdb_id_clean = imdb_id_raw.replace('.0', '').replace('.', '').lstrip('t')
            if imdb_id_clean.isdigit():
                imdb_id_clean = f"tt{imdb_id_clean.zfill(7)}"
            else:
                imdb_id_clean = imdb_id_raw
            
            if imdb_id_clean != imdb_id_raw:
                logger.info(f"[SEARCH MOVIES] ID преобразован: '{imdb_id_raw}' → '{imdb_id_clean}'")
            
            distance = candidate_distances[i]
            
            has_overview = row.get('has_overview', False) if 'has_overview' in row.index else False
            overview_boost = 30 if has_overview else 0  # бонус за наличие overview
            
            # ПРИОРИТЕТ №1: Буст за полное имя актёра/режиссёра (+400 если найдено) - САМЫЙ СИЛЬНЫЙ
            # Сначала проверяем актёров (приоритет №1), потом режиссёров (приоритет №2)
            actor_boost = 0
            if mentioned_actor_en:
                # Нормализуем тексты для сравнения
                actors_normalized = _normalize_text(row.get('actors_str', '')) if 'actors_str' in row.index else ''
                director_normalized = _normalize_text(row.get('director_str', '')) if 'director_str' in row.index else ''
                actor_name_for_search = _normalize_text(mentioned_actor_en)
                
                # Проверяем, является ли имя актёром или режиссёром из топ-списков
                is_actor = actor_name_for_search in top_actors_set
                is_director = actor_name_for_search in top_directors_set
                
                # ПРИОРИТЕТ №1: Актёры (если имя в топ-актёрах или просто проверяем)
                if is_actor and actor_name_for_search in actors_normalized:
                    actor_boost = 400
                    logger.info(f"[SEARCH MOVIES] Полное имя актёра '{actor_name_for_search}' найдено → +400 для {imdb_id_clean}")
                # ПРИОРИТЕТ №2: Режиссёры (такой же буст, если актёров нет)
                elif (is_director or not is_actor) and actor_name_for_search in director_normalized:
                    actor_boost = 400
                    logger.info(f"[SEARCH MOVIES] Полное имя режиссёра '{actor_name_for_search}' найдено → +400 для {imdb_id_clean}")
            
            # ПРИОРИТЕТ №2: Keyword-матчинг по overview (×25 за каждое совпадение)
            overview_keyword_matches = 0
            if keywords and 'overview' in row.index:
                overview_text_normalized = _normalize_text(row.get('overview', ''))
                overview_keyword_matches = sum(1 for word in keywords if word in overview_text_normalized)
            
            # ПРИОРИТЕТ №3: Буст за жанр (если жанр упомянут в запросе и есть в фильме)
            genre_boost = 0
            if detected_genres and 'genres_str' in row.index:
                genres_str_normalized = _normalize_text(row.get('genres_str', ''))
                for genre in detected_genres:
                    if genre in genres_str_normalized:
                        genre_boost += 100  # Сильный буст за каждый совпадающий жанр
                        logger.info(f"[SEARCH MOVIES] Жанр '{genre}' найден → +100 для {imdb_id_clean}")
            
            # ПРИОРИТЕТ №4: Keyword-матчинг по названию (небольшой буст)
            title_keyword_matches = 0
            title_boost = 0
            if keywords and 'title' in row.index:
                title_text_normalized = _normalize_text(row.get('title', ''))
                title_keyword_matches = sum(1 for word in keywords if word in title_text_normalized)
                # Небольшой буст за совпадения в названии (не очень сильный)
                if title_keyword_matches > 0:
                    title_boost = 5 * title_keyword_matches  # Небольшой буст за совпадения в названии
            
            # Буст по популярности (vote_count)
            popularity_boost = 0
            if 'vote_count' in row.index:
                vote_count = row.get('vote_count', 0)
                if pd.notna(vote_count) and isinstance(vote_count, (int, float)):
                    # Линейный буст: min(vote_count / 1000, 150) - максимум +150
                    popularity_boost = min(float(vote_count) / 1000.0, 150.0)
            
            # Буст за свежесть (год > 2000)
            freshness_boost = 0
            year = row.get('year')
            if pd.notna(year) and isinstance(year, (int, float)):
                year_int = int(year)
                if year_int > 2000:
                    freshness_boost = 25  # бонус за свежесть
            
            # Базовый семантический score
            base_score = 1.0 - distance
            
            # Итоговый score с правильными приоритетами:
            # ПРИОРИТЕТ №1: actor_boost (+400) - САМЫЙ СИЛЬНЫЙ
            # ПРИОРИТЕТ №2: overview_keyword_matches (×25)
            # ПРИОРИТЕТ №3: genre_boost (+100 за жанр)
            # ПРИОРИТЕТ №4: title_boost (+5 за совпадение в названии)
            score = base_score + actor_boost + (overview_keyword_matches * 25.0) + genre_boost + title_boost + overview_boost + freshness_boost + popularity_boost
            
            results.append({
                'imdb_id': imdb_id_clean,
                'title': row['title'],
                'year': row['year'] if pd.notna(row['year']) else None,
                'description': row['description'][:500] if 'description' in row.index else '',
                'distance': distance,
                'has_overview': has_overview,
                'overview_keyword_matches': overview_keyword_matches,
                'overview_boost': overview_boost,
                'freshness_boost': freshness_boost,
                'popularity_boost': popularity_boost,
                'actor_boost': actor_boost,
                'genre_boost': genre_boost,
                'title_boost': title_boost,
                'score': score
            })
        
        results.sort(key=lambda x: x['score'], reverse=True)
        results = results[:top_k]
        
        logger.info(f"[SEARCH MOVIES] Результаты приоритизированы, возвращаем {len(results)} фильмов")
        if results:
            logger.info(f"[SEARCH MOVIES] Топ-3: {[(r['title'], r['actor_boost'], r['overview_keyword_matches'], r['genre_boost'], r['title_boost'], r['score']) for r in results[:3]]}")
        return results
    except Exception as e:
        logger.error(f"[SEARCH MOVIES] Ошибка поиска фильмов: {e}", exc_info=True)
        return []