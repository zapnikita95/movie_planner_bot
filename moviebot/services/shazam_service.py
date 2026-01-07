"""
Сервис для поиска фильмов по описанию (КиноШазам)
Использует IMDB базу данных, semantic search, переводчик и распознавание речи
"""
import os
import logging
import pandas as pd
import numpy as np
import faiss
import requests
import gzip
import shutil
from pathlib import Path
from sentence_transformers import SentenceTransformer
from transformers import pipeline
from pydub import AudioSegment
import torch
import gc
import time
import sys

# Настройка для предотвращения segmentation fault на macOS
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'

logger = logging.getLogger(__name__)

# Глобальные переменные для кэширования моделей
_model = None
_translator = None
_whisper = None
_vosk = None
_index = None
_movies_df = None

# Путь к данным
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'shazam'
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Кэш для моделей (Railway Volume)
CACHE_DIR = Path(os.getenv('CACHE_DIR', '/app/cache'))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Настраиваем кэш для HuggingFace моделей
os.environ['HF_HOME'] = str(CACHE_DIR / 'huggingface')
os.environ['TRANSFORMERS_CACHE'] = str(CACHE_DIR / 'huggingface' / 'transformers')
os.environ['SENTENCE_TRANSFORMERS_HOME'] = str(CACHE_DIR / 'huggingface' / 'sentence_transformers')

logger.info(f"[CACHE] Используется кэш моделей: {CACHE_DIR}")

# Кэш для IMDB данных
IMDB_CACHE_DIR = CACHE_DIR / 'imdb'
IMDB_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Индексы и обработанные данные остаются в DATA_DIR (могут пересоздаваться)
INDEX_PATH = DATA_DIR / 'imdb_index.faiss'
DATA_PATH = DATA_DIR / 'imdb_movies.csv'

# IMDB исходные данные кэшируются в volume
BASICS_PATH = IMDB_CACHE_DIR / 'title.basics.tsv'
RATINGS_PATH = IMDB_CACHE_DIR / 'title.ratings.tsv'
PLOT_PATH = IMDB_CACHE_DIR / 'title.plot.summary.tsv'
KEYWORDS_PATH = IMDB_CACHE_DIR / 'keywords.list'
KEYWORDS_TSV_PATH = IMDB_CACHE_DIR / 'title.keywords.tsv'

# Настройки фильтрации
MIN_VOTES = 5000  # Минимальное количество голосов для популярности


def get_model():
    """Ленивая загрузка модели embeddings"""
    global _model
    if _model is None:
        logger.info("Загрузка модели embeddings...")
        try:
            _model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            logger.info("Модель embeddings загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки модели embeddings: {e}", exc_info=True)
            raise
    return _model


def get_translator():
    """Ленивая загрузка переводчика русский → английский"""
    global _translator
    if _translator is None:
        logger.info("Загрузка транслятора русский → английский...")
        try:
            # Настройки для предотвращения segmentation fault
            torch.set_num_threads(1)
            torch.set_grad_enabled(False)
            
            _translator = pipeline(
                "translation",
                model="Helsinki-NLP/opus-mt-ru-en",
                device=-1,  # CPU
                torch_dtype=torch.float32
            )
            
            # Тестовый перевод для проверки
            test_result = _translator("тест", max_length=512)
            logger.info(f"Транслятор загружен успешно (тест: 'тест' → '{test_result[0]['translation_text']}')")
        except Exception as e:
            logger.error(f"Ошибка загрузки транслятора: {e}", exc_info=True)
            logger.warning("Перевод будет отключен, используем оригинальные запросы")
            _translator = False  # Помечаем как недоступный
    return _translator


def get_whisper():
    """Ленивая загрузка модели Whisper для распознавания речи"""
    global _whisper
    if _whisper is None:
        logger.info(f"Загрузка модели Whisper (кэш: {CACHE_DIR})...")
        try:
            # Используем whisper.load_model для возможности указать download_root
            import whisper
            
            # Создаем директорию для кэша Whisper
            whisper_cache_dir = CACHE_DIR / 'whisper'
            whisper_cache_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Загружаем Whisper base модель в {whisper_cache_dir}...")
            whisper_model = whisper.load_model("base", download_root=str(whisper_cache_dir))
            
            # Создаем обертку для совместимости с pipeline API
            class WhisperWrapper:
                def __init__(self, model):
                    self.model = model
                
                def __call__(self, audio_path):
                    result = self.model.transcribe(audio_path, language="ru")
                    return {"text": result.get("text", "").strip()}
            
            _whisper = WhisperWrapper(whisper_model)
            logger.info("Модель Whisper загружена из кэша/скачана")
        except ImportError:
            logger.warning("Whisper не установлен. Установите: pip install openai-whisper")
            # Fallback на transformers pipeline
            try:
                logger.info("Пробуем загрузить через transformers pipeline...")
                _whisper = pipeline(
                    "automatic-speech-recognition",
                    model="openai/whisper-small",
                    device=-1  # CPU
                )
                logger.info("Модель Whisper загружена через transformers")
            except Exception as e:
                logger.error(f"Ошибка загрузки Whisper через pipeline: {e}", exc_info=True)
                _whisper = False
        except Exception as e:
            logger.error(f"Ошибка загрузки Whisper: {e}", exc_info=True)
            _whisper = False  # Помечаем как недоступный
    return _whisper


def get_vosk():
    """Ленивая загрузка модели Vosk для распознавания речи (fallback)"""
    global _vosk
    if _vosk is None:
        logger.info("Загрузка модели Vosk...")
        try:
            import vosk
            import json
            
            # Путь к модели Vosk (нужно скачать отдельно)
            vosk_model_path = DATA_DIR / 'vosk-model-small-ru-0.22'
            
            if not vosk_model_path.exists():
                logger.warning(f"Модель Vosk не найдена по пути {vosk_model_path}")
                logger.info("Скачайте модель с https://alphacephei.com/vosk/models")
                _vosk = False
                return _vosk
            
            model = vosk.Model(str(vosk_model_path))
            _vosk = vosk.KaldiRecognizer(model, 16000)  # 16kHz sample rate
            _vosk.SetWords(True)
            logger.info("Модель Vosk загружена")
        except ImportError:
            logger.warning("Vosk не установлен. Установите: pip install vosk")
            _vosk = False
        except Exception as e:
            logger.error(f"Ошибка загрузки Vosk: {e}", exc_info=True)
            _vosk = False
    return _vosk


def translate_to_english(text):
    """Переводит русский текст на английский"""
    translator = get_translator()
    if not translator or translator is False:
        return text
    
    # Простая проверка на русский
    russian_chars = set('абвгдеёжзийклмнопрстуфхцчшщъыьэюя')
    if any(c.lower() in russian_chars for c in text):
        try:
            result = translator(text, max_length=512)
            translated = result[0]['translation_text']
            logger.info(f"Переведено: '{text}' → '{translated}'")
            return translated
        except Exception as e:
            logger.warning(f"Ошибка перевода: {e}")
            return text
    return text


def transcribe_with_whisper(audio_path):
    """Распознает речь с помощью Whisper"""
    logger.info(f"[WHISPER] Начинаем распознавание: {audio_path}")
    whisper = get_whisper()
    if not whisper or whisper is False:
        logger.warning(f"[WHISPER] Модель Whisper недоступна")
        return None
    
    try:
        logger.info(f"[WHISPER] Вызываем модель Whisper...")
        result = whisper(audio_path)
        logger.info(f"[WHISPER] Модель вернула результат: {type(result)}")
        text = result.get("text", "").strip()
        if text:
            logger.info(f"[WHISPER] Распознано: '{text}'")
            return text
        logger.warning(f"[WHISPER] Пустой результат распознавания")
        return None
    except Exception as e:
        logger.error(f"[WHISPER] Ошибка распознавания: {e}", exc_info=True)
        return None


def transcribe_with_vosk(audio_path):
    """Распознает речь с помощью Vosk (fallback)"""
    vosk_model = get_vosk()
    if not vosk_model or vosk_model is False:
        return None
    
    try:
        import soundfile as sf
        import json
        
        # Читаем аудио файл
        data, sample_rate = sf.read(audio_path)
        
        # Конвертируем в моно и 16kHz если нужно
        if len(data.shape) > 1:
            data = np.mean(data, axis=1)  # Стерео → моно
        
        if sample_rate != 16000:
            # Простая передискретизация (для точности лучше использовать librosa)
            import librosa
            data = librosa.resample(data, orig_sr=sample_rate, target_sr=16000)
            sample_rate = 16000
        
        # Конвертируем в int16
        data_int16 = (data * 32768).astype(np.int16)
        
        # Распознаем
        vosk_model = get_vosk()  # Получаем заново, так как может быть перезаписан
        if not vosk_model or vosk_model is False:
            return None
        
        text_parts = []
        chunk_size = 4000
        
        for i in range(0, len(data_int16), chunk_size):
            chunk = data_int16[i:i+chunk_size].tobytes()
            if vosk_model.AcceptWaveform(chunk):
                result = json.loads(vosk_model.Result())
                if 'text' in result and result['text']:
                    text_parts.append(result['text'])
        
        # Финальный результат
        final_result = json.loads(vosk_model.FinalResult())
        if 'text' in final_result and final_result['text']:
            text_parts.append(final_result['text'])
        
        text = ' '.join(text_parts).strip()
        if text:
            logger.info(f"Vosk распознал: '{text}'")
            return text
        return None
    except Exception as e:
        logger.error(f"Ошибка распознавания Vosk: {e}", exc_info=True)
        return None


def download_file_with_progress(url, filepath):
    """Скачивает файл с прогресс-баром"""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Проверяем Content-Type
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' in content_type:
            logger.error(f"Получен HTML вместо файла для {url}")
            return False
        
        # Проверяем magic bytes для gzip
        first_bytes = response.content[:2]
        if filepath.suffix == '.gz' and first_bytes != b'\x1f\x8b':
            logger.error(f"Файл {url} не является gzip архивом")
            return False
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                from tqdm import tqdm
                downloaded = 0
                for chunk in tqdm(response.iter_content(chunk_size=8192), 
                                 total=total_size // 8192, 
                                 unit='KB', 
                                 desc=f"Скачивание {filepath.name}"):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
        
        logger.info(f"Файл {filepath.name} скачан")
        return True
    except Exception as e:
        logger.error(f"Ошибка скачивания {url}: {e}", exc_info=True)
        return False


def extract_gzip_with_progress(gz_path, tsv_path):
    """Распаковывает gzip файл с прогресс-баром"""
    try:
        file_size = os.path.getsize(gz_path)
        with gzip.open(gz_path, 'rb') as f_in:
            with open(tsv_path, 'wb') as f_out:
                from tqdm import tqdm
                shutil.copyfileobj(f_in, f_out, length=8192)
        logger.info(f"Файл {tsv_path.name} распакован")
        return True
    except Exception as e:
        logger.error(f"Ошибка распаковки {gz_path}: {e}", exc_info=True)
        return False


def download_imdb_data():
    """Скачивает и распаковывает датасеты IMDB, если они отсутствуют"""
    logger.info(f"Проверка данных IMDB в кэше: {IMDB_CACHE_DIR}")
    
    datasets = [
        ('https://datasets.imdbws.com/title.basics.tsv.gz', 'title.basics.tsv.gz', BASICS_PATH),
        ('https://datasets.imdbws.com/title.ratings.tsv.gz', 'title.ratings.tsv.gz', RATINGS_PATH),
        ('https://datasets.imdbws.com/title.plot.summary.tsv.gz', 'title.plot.summary.tsv.gz', PLOT_PATH),
        ('https://datasets.imdbws.com/title.keywords.tsv.gz', 'title.keywords.tsv.gz', KEYWORDS_TSV_PATH),
    ]
    
    # Пробуем скачать keywords.list (старый формат) если TSV недоступен
    keywords_list_url = 'https://datasets.imdbws.com/keywords.list.gz'
    keywords_list_gz_path = IMDB_CACHE_DIR / 'keywords.list.gz'
    
    for url, gz_file, tsv_path in datasets:
        gz_path = IMDB_CACHE_DIR / gz_file
        
        # Проверяем, существует ли уже распакованный файл
        if tsv_path.exists():
            logger.info(f"{tsv_path.name} уже существует в кэше — пропускаем")
            continue
        
        # Если TSV нет, проверяем наличие gz файла
        if not gz_path.exists():
            logger.info(f"Скачивание {gz_file}...")
            if not download_file_with_progress(url, gz_path):
                # Для keywords пробуем старый формат
                if 'keywords' in gz_file:
                    logger.info("Пробуем скачать keywords.list (старый формат)...")
                    if download_file_with_progress(keywords_list_url, keywords_list_gz_path):
                        if extract_gzip_with_progress(keywords_list_gz_path, KEYWORDS_PATH):
                            logger.info(f"{KEYWORDS_PATH.name} скачан и распакован")
                            continue
                logger.warning(f"Не удалось скачать {gz_file}, пропускаем")
                continue
        
        # Распаковываем gz файл
        logger.info(f"Распаковка {gz_file}...")
        if extract_gzip_with_progress(gz_path, tsv_path):
            logger.info(f"{gz_file} скачан и распакован в {tsv_path}")
        else:
            logger.warning(f"Не удалось распаковать {gz_file}")


def parse_keywords_list(keywords_path):
    """Парсит keywords.list (старый формат)"""
    keywords_dict = {}
    try:
        with open(keywords_path, 'r', encoding='utf-8', errors='ignore') as f:
            current_imdb_id = None
            for line in f:
                line = line.strip()
                if line.startswith('MV:'):
                    # Новая запись фильма
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        current_imdb_id = parts[1].strip()
                        if current_imdb_id not in keywords_dict:
                            keywords_dict[current_imdb_id] = []
                elif line.startswith('KW:') and current_imdb_id:
                    # Ключевое слово
                    keyword = line.replace('KW:', '').strip()
                    if keyword:
                        keywords_dict[current_imdb_id].append(keyword)
    except Exception as e:
        logger.error(f"Ошибка парсинга keywords.list: {e}", exc_info=True)
    return keywords_dict


def parse_keywords_tsv(keywords_path):
    """Парсит title.keywords.tsv"""
    keywords_dict = {}
    try:
        df = pd.read_csv(keywords_path, sep='\t', low_memory=False)
        for _, row in df.iterrows():
            tconst = row.get('tconst', '')
            keywords_str = row.get('keywords', '')
            if tconst and keywords_str and keywords_str != '\\N':
                # Ключевые слова разделены запятыми
                keywords = [k.strip() for k in keywords_str.split(',') if k.strip()]
                if keywords:
                    keywords_dict[tconst] = keywords
    except Exception as e:
        logger.error(f"Ошибка парсинга title.keywords.tsv: {e}", exc_info=True)
    return keywords_dict


def build_imdb_index():
    """Создает индекс для поиска фильмов"""
    global _index, _movies_df
    
    # Проверяем, нужно ли пересоздавать индекс
    if INDEX_PATH.exists() and DATA_PATH.exists():
        try:
            _index = faiss.read_index(str(INDEX_PATH))
            _movies_df = pd.read_csv(DATA_PATH)
            logger.info(f"Загружен существующий индекс: {len(_movies_df)} фильмов")
            return _index, _movies_df
        except Exception as e:
            logger.warning(f"Ошибка загрузки индекса: {e}, пересоздаем...")
    
    download_imdb_data()
    
    logger.info("Загружаем IMDB данные...")
    
    # Загружаем basics
    if not BASICS_PATH.exists():
        logger.error(f"Файл {BASICS_PATH} не найден")
        return None, None
    
    basics = pd.read_csv(BASICS_PATH, sep='\t', low_memory=False)
    logger.info(f"Загружено {len(basics)} записей из basics")
    
    # Загружаем ratings
    ratings = None
    if RATINGS_PATH.exists():
        ratings = pd.read_csv(RATINGS_PATH, sep='\t', low_memory=False)
        logger.info(f"Загружено {len(ratings)} записей из ratings")
    else:
        logger.warning(f"Файл {RATINGS_PATH} не найден, фильтрация по рейтингу недоступна")
    
    # Загружаем plot (опционально)
    plots = None
    if PLOT_PATH.exists():
        plots = pd.read_csv(PLOT_PATH, sep='\t', low_memory=False)
        logger.info(f"Загружено {len(plots)} записей из plots")
    else:
        logger.warning(f"Файл {PLOT_PATH} не найден, работаем без описаний сюжета")
    
    # Загружаем keywords (опционально)
    keywords_dict = {}
    if KEYWORDS_TSV_PATH.exists():
        logger.info("Парсим keywords из TSV...")
        keywords_dict = parse_keywords_tsv(KEYWORDS_TSV_PATH)
        logger.info(f"Загружено ключевых слов для {len(keywords_dict)} фильмов")
    elif KEYWORDS_PATH.exists():
        logger.info("Парсим keywords из list...")
        keywords_dict = parse_keywords_list(KEYWORDS_PATH)
        logger.info(f"Загружено ключевых слов для {len(keywords_dict)} фильмов")
    else:
        logger.warning("Файлы keywords не найдены, работаем без ключевых слов")
    
    # Фильтруем только фильмы
    movies = basics[basics['titleType'] == 'movie'].copy()
    logger.info(f"Фильтровано {len(movies)} фильмов")
    
    # Объединяем с рейтингами
    if ratings is not None:
        movies = movies.merge(ratings[['tconst', 'averageRating', 'numVotes']], on='tconst', how='left')
        # Фильтруем по минимальному количеству голосов
        movies = movies[movies['numVotes'] >= MIN_VOTES]
        # Сортируем по популярности
        movies = movies.sort_values('numVotes', ascending=False)
        logger.info(f"После фильтрации по рейтингу: {len(movies)} фильмов")
    
    # Объединяем с описаниями (если есть)
    if plots is not None:
        movies = movies.merge(plots[['tconst', 'plot']], on='tconst', how='left')
        movies = movies.dropna(subset=['plot'])
        logger.info(f"После объединения с описаниями: {len(movies)} фильмов")
    else:
        # Если нет описаний, используем только название и год
        movies['plot'] = movies['primaryTitle'] + ' (' + movies['startYear'].astype(str) + ')'
        logger.info(f"Работаем без описаний сюжета, используем только названия: {len(movies)} фильмов")
    
    # Добавляем ключевые слова к описаниям (если есть)
    def enrich_description(row):
        plot = str(row.get('plot', ''))
        tconst = row.get('tconst', '')
        keywords = keywords_dict.get(tconst, [])
        if keywords:
            # Добавляем до 10 ключевых слов
            keywords_str = ', '.join(keywords[:10])
            return f"{plot} Keywords: {keywords_str}"
        return plot
    
    movies['description'] = movies.apply(enrich_description, axis=1)
    
    # Подготавливаем данные
    movies['title'] = movies['primaryTitle']
    movies['year'] = movies['startYear'].astype('Int64')
    movies['imdb_id'] = movies['tconst']
    
    # Ограничиваем количество для индексации (берем топ популярных)
    movies = movies.head(50000)  # Максимум 50k фильмов
    
    logger.info(f"Создаём индекс для {len(movies)} фильмов...")
    
    # Создаем embeddings
    model = get_model()
    descriptions = movies['description'].tolist()
    
    from tqdm import tqdm
    embeddings = []
    batch_size = 32
    for i in tqdm(range(0, len(descriptions), batch_size), desc="Создание embeddings"):
        batch = descriptions[i:i+batch_size]
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        embeddings.extend(batch_embeddings)
    
    embeddings = np.array(embeddings).astype('float32')
    dimension = embeddings.shape[1]
    
    # Создаем индекс
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings)
    
    # Сохраняем
    faiss.write_index(index, str(INDEX_PATH))
    movies[['imdb_id', 'title', 'year', 'description']].to_csv(DATA_PATH, index=False)
    
    _index = index
    _movies_df = movies
    
    logger.info(f"Индекс создан и сохранён: {len(movies)} фильмов")
    return index, movies


def get_index_and_movies():
    """Получает индекс и данные о фильмах (ленивая загрузка)"""
    global _index, _movies_df
    if _index is None or _movies_df is None:
        _index, _movies_df = build_imdb_index()
    return _index, _movies_df


def search_movies(query, top_k=5):
    """Ищет фильмы по запросу"""
    try:
        # Переводим запрос на английский
        query_en = translate_to_english(query)
        
        # Получаем индекс и данные
        index, movies = get_index_and_movies()
        if index is None or movies is None:
            logger.error("Индекс не создан")
            return []
        
        # Создаем embedding для запроса
        model = get_model()
        query_emb = model.encode([query_en])[0].astype('float32').reshape(1, -1)
        
        # Ищем
        D, I = index.search(query_emb, k=top_k)
        
        # Формируем результаты
        results = []
        for idx in I[0]:
            if idx < len(movies):
                row = movies.iloc[idx]
                results.append({
                    'imdb_id': row['imdb_id'],
                    'title': row['title'],
                    'year': row['year'] if pd.notna(row['year']) else None,
                    'description': row['description'][:300] if len(str(row['description'])) > 300 else row['description']
                })
        
        return results
    except Exception as e:
        logger.error(f"Ошибка поиска фильмов: {e}", exc_info=True)
        return []


def transcribe_voice(audio_path):
    """Распознает речь из аудио файла (Whisper → Vosk)"""
    logger.info(f"[TRANSCRIBE] Начинаем распознавание: {audio_path}")
    
    # Пробуем Whisper сначала
    logger.info(f"[TRANSCRIBE] Пробуем Whisper...")
    try:
        text = transcribe_with_whisper(audio_path)
        if text:
            logger.info(f"[TRANSCRIBE] Whisper успешно распознал: '{text}'")
            return text
        logger.warning(f"[TRANSCRIBE] Whisper вернул пустой результат")
    except Exception as e:
        logger.error(f"[TRANSCRIBE] Ошибка при распознавании Whisper: {e}", exc_info=True)
    
    # Если Whisper не сработал, пробуем Vosk
    logger.info(f"[TRANSCRIBE] Whisper не распознал, пробуем Vosk...")
    try:
        text = transcribe_with_vosk(audio_path)
        if text:
            logger.info(f"[TRANSCRIBE] Vosk успешно распознал: '{text}'")
            return text
        logger.warning(f"[TRANSCRIBE] Vosk вернул пустой результат")
    except Exception as e:
        logger.error(f"[TRANSCRIBE] Ошибка при распознавании Vosk: {e}", exc_info=True)
    
    logger.warning(f"[TRANSCRIBE] Не удалось распознать речь ни Whisper, ни Vosk")
    return None


def convert_ogg_to_wav(ogg_path, wav_path, sample_rate=16000):
    """Конвертирует OGG в WAV (16kHz, mono)"""
    try:
        audio = AudioSegment.from_ogg(ogg_path)
        audio = audio.set_frame_rate(sample_rate)
        audio = audio.set_channels(1)  # Mono
        audio.export(wav_path, format="wav")
        return True
    except Exception as e:
        logger.error(f"Ошибка конвертации OGG в WAV: {e}", exc_info=True)
        return False

