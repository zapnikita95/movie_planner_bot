"""
Сервис для поиска фильмов через IMDB (КиноШазам)
Использует семантический поиск для поиска фильмов по описанию
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
import requests
import gzip
import shutil
from tqdm import tqdm

# Отключаем MPS и multiprocessing для избежания segmentation fault на macOS
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'

# Опция для полного отключения перевода (если возникают проблемы)
DISABLE_TRANSLATION = os.environ.get('DISABLE_TRANSLATION', 'false').lower() == 'true'

logger = logging.getLogger(__name__)

# Путь к данным (относительно корня проекта)
# На Railway используем /data если доступно (Railway Volume), иначе локальная папка
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.exists('/data'):
    # Railway Volume для постоянного хранения
    DATA_DIR = '/data/shazam'
else:
    # Локальное хранилище (для разработки)
    DATA_DIR = os.path.join(BASE_DIR, 'data', 'shazam')
os.makedirs(DATA_DIR, exist_ok=True)
logger.info(f"[SHAZAM] Используется директория данных: {DATA_DIR}")

INDEX_PATH = os.path.join(DATA_DIR, 'imdb_index.faiss')
DATA_PATH = os.path.join(DATA_DIR, 'imdb_movies.csv')

# Глобальные переменные для моделей (lazy loading)
_model = None
_translator = None
_whisper = None
_vosk = None
_index = None
_movies = None

def get_model():
    """Ленивая загрузка модели embeddings"""
    global _model
    if _model is None:
        logger.info("Загрузка модели embeddings...")
        _model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    return _model

def get_translator():
    """Ленивая загрузка транслятора с безопасной инициализацией"""
    global _translator
    if DISABLE_TRANSLATION:
        logger.info("Перевод отключен через переменную окружения DISABLE_TRANSLATION")
        return None
    if _translator is None:
        logger.info("Загрузка транслятора русский → английский...")
        try:
            import torch
            import gc
            import warnings
            import time
            
            warnings.filterwarnings('ignore')
            torch.set_num_threads(1)
            if hasattr(torch.backends, 'mps'):
                os.environ['PYTORCH_MPS_HIGH_WATERMARK_RATIO'] = '0.0'
                try:
                    torch.backends.mps.is_available = lambda: False
                    torch.backends.mps.is_built = lambda: False
                except:
                    pass
            
            torch.set_grad_enabled(False)
            
            from transformers import pipeline, AutoModelForSeq2SeqLM, AutoTokenizer
            
            model_name = "Helsinki-NLP/opus-mt-ru-en"
            
            logger.info(f"Загрузка токенизатора {model_name}...")
            try:
                tokenizer = AutoTokenizer.from_pretrained(
                    model_name,
                    use_fast=False,
                    local_files_only=False,
                    trust_remote_code=False
                )
            except:
                tokenizer = AutoTokenizer.from_pretrained(
                    model_name,
                    use_fast=True,
                    local_files_only=False
                )
            
            time.sleep(0.3)
            gc.collect()
            
            logger.info(f"Загрузка модели {model_name}...")
            os.environ['PYTORCH_MPS_HIGH_WATERMARK_RATIO'] = '0.0'
            os.environ['PYTORCH_ENABLE_MPS_FALLBACK'] = '1'
            
            with torch.no_grad():
                try:
                    model = AutoModelForSeq2SeqLM.from_pretrained(
                        model_name,
                        torch_dtype=torch.float32,
                        device_map='cpu',
                        low_cpu_mem_usage=True,
                        trust_remote_code=False
                    )
                except:
                    model = AutoModelForSeq2SeqLM.from_pretrained(
                        model_name,
                        torch_dtype=torch.float32,
                        low_cpu_mem_usage=True
                    )
                    model = model.to('cpu')
                
                model.eval()
            
            time.sleep(0.3)
            gc.collect()
            
            logger.info("Создание pipeline...")
            _translator = pipeline(
                "translation",
                model=model,
                tokenizer=tokenizer,
                device=-1,
                framework="pt",
                return_tensors=False
            )
            
            gc.collect()
            torch.set_grad_enabled(True)
            
            logger.info("Транслятор загружен успешно!")
        except Exception as e:
            logger.error(f"Ошибка загрузки транслятора: {e}")
            _translator = False
    return _translator if _translator is not False else None

def translate_to_english(text):
    """Переводит русский текст на английский для лучшего поиска в IMDB"""
    russian_chars = set('абвгдеёжзийклмнопрстуфхцчшщъыьэюя')
    if any(c.lower() in russian_chars for c in text):
        translator = get_translator()
        if translator is None:
            logger.warning("Транслятор недоступен, используем оригинальный запрос")
            return text
        try:
            result = translator(text, max_length=512)
            translated = result[0]['translation_text']
            logger.info(f"Переведено: '{text}' → '{translated}'")
            return translated
        except Exception as e:
            logger.warning(f"Ошибка перевода: {e}, используем оригинальный запрос")
            return text
    return text

def download_file_with_progress(url, filepath, description):
    """Скачивает файл с прогресс-баром"""
    if os.path.exists(filepath):
        logger.info(f"{description} уже существует, пропускаем загрузку")
        return True
    
    logger.info(f"Скачиваем {description}...")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '')
        if 'text/html' in content_type:
            logger.error(f"Ошибка: {url} вернул HTML вместо файла")
            return False
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            if total_size > 0:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=description) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        logger.info(f"{description} успешно скачан")
        return True
    except Exception as e:
        logger.error(f"Ошибка при скачивании {description}: {e}")
        if os.path.exists(filepath):
            os.remove(filepath)
        return False

def extract_gzip_with_progress(gz_path, tsv_path, description):
    """Распаковывает gzip файл с проверкой и прогресс-баром"""
    if os.path.exists(tsv_path):
        logger.info(f"{description} уже распакован, пропускаем")
        return True
    
    logger.info(f"Распаковываем {description}...")
    try:
        with open(gz_path, 'rb') as f:
            magic = f.read(2)
            if magic != b'\x1f\x8b':
                logger.error(f"Ошибка: {gz_path} не является gzip файлом")
                return False
        
        file_size = os.path.getsize(gz_path)
        with gzip.open(gz_path, 'rb') as f_in:
            with open(tsv_path, 'wb') as f_out:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Распаковка {description}") as pbar:
                    while True:
                        chunk = f_in.read(8192)
                        if not chunk:
                            break
                        f_out.write(chunk)
                        pbar.update(len(chunk))
        
        logger.info(f"{description} успешно распакован")
        return True
    except Exception as e:
        logger.error(f"Ошибка при распаковке {description}: {e}")
        return False

def parse_keywords_list(keywords_path):
    """
    Парсит keywords.list файл и возвращает словарь tconst -> [keywords]
    
    Формат файла:
    tt1234567: keyword1
    tt1234567: keyword2
    tt7890123: keyword3
    """
    keywords_dict = {}
    
    if not os.path.exists(keywords_path):
        logger.warning(f"Файл keywords.list не найден: {keywords_path}")
        return keywords_dict
    
    logger.info("Парсим keywords.list...")
    try:
        with open(keywords_path, 'r', encoding='utf-8', errors='ignore') as f:
            line_count = 0
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Формат: tt1234567: keyword
                if ':' in line:
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        tconst = parts[0].strip()
                        keyword = parts[1].strip()
                        
                        if tconst.startswith('tt') and keyword:
                            if tconst not in keywords_dict:
                                keywords_dict[tconst] = []
                            keywords_dict[tconst].append(keyword)
                            line_count += 1
                            
                            if line_count % 100000 == 0:
                                logger.info(f"Обработано {line_count} ключевых слов...")
        
        logger.info(f"Загружено {len(keywords_dict)} фильмов с ключевыми словами (всего {line_count} ключевых слов)")
        return keywords_dict
    except Exception as e:
        logger.error(f"Ошибка при парсинге keywords.list: {e}", exc_info=True)
        return keywords_dict

def parse_keywords_tsv(keywords_tsv_path):
    """
    Парсит keywords TSV файл (если доступен) и возвращает словарь tconst -> [keywords]
    
    Формат TSV: tconst, keyword
    """
    keywords_dict = {}
    
    if not os.path.exists(keywords_tsv_path):
        return keywords_dict
    
    logger.info("Парсим keywords TSV...")
    try:
        keywords_df = pd.read_csv(keywords_tsv_path, sep='\t', low_memory=False, header=None, names=['tconst', 'keyword'])
        
        for _, row in keywords_df.iterrows():
            tconst = str(row['tconst']).strip()
            keyword = str(row['keyword']).strip()
            
            if tconst.startswith('tt') and keyword and keyword != 'nan':
                if tconst not in keywords_dict:
                    keywords_dict[tconst] = []
                keywords_dict[tconst].append(keyword)
        
        logger.info(f"Загружено {len(keywords_dict)} фильмов с ключевыми словами из TSV")
        return keywords_dict
    except Exception as e:
        logger.warning(f"Ошибка при парсинге keywords TSV: {e}")
        return keywords_dict

def download_imdb_data():
    basics_url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    ratings_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    plot_url = 'https://datasets.imdbws.com/title.plot.summary.tsv.gz'
    # Пробуем несколько возможных URL для keywords
    keywords_urls = [
        'https://datasets.imdbws.com/keywords.list.gz',
        'https://www.imdb.com/interfaces/keywords.list.gz',
        'https://datasets.imdbws.com/title.keywords.tsv.gz',  # Альтернативный формат
    ]

    basics_path = os.path.join(DATA_DIR, 'title.basics.tsv.gz')
    ratings_path = os.path.join(DATA_DIR, 'title.ratings.tsv.gz')
    plot_path = os.path.join(DATA_DIR, 'title.plot.summary.tsv.gz')
    keywords_path_gz = os.path.join(DATA_DIR, 'keywords.list.gz')
    keywords_path = os.path.join(DATA_DIR, 'keywords.list')

    if not download_file_with_progress(basics_url, basics_path, "IMDB basics"):
        raise Exception("Не удалось скачать IMDB basics")
    
    ratings_available = download_file_with_progress(ratings_url, ratings_path, "IMDB ratings")
    if not ratings_available:
        logger.warning("IMDB ratings недоступен")
    
    plots_available = download_file_with_progress(plot_url, plot_path, "IMDB plots")
    if not plots_available:
        logger.warning("IMDB plots недоступен")
    
    # Пробуем скачать keywords
    keywords_available = False
    keywords_tsv_path = os.path.join(DATA_DIR, 'title.keywords.tsv')
    keywords_tsv_path_gz = os.path.join(DATA_DIR, 'title.keywords.tsv.gz')
    
    # Сначала пробуем TSV формат (более структурированный)
    if not os.path.exists(keywords_tsv_path):
        keywords_tsv_url = 'https://datasets.imdbws.com/title.keywords.tsv.gz'
        if download_file_with_progress(keywords_tsv_url, keywords_tsv_path_gz, "IMDB keywords TSV"):
            keywords_available = True
            if extract_gzip_with_progress(keywords_tsv_path_gz, keywords_tsv_path, "keywords TSV"):
                logger.info("Keywords TSV успешно загружен")
            else:
                keywords_available = False
    else:
        keywords_available = True
        logger.info("keywords TSV уже существует")
    
    # Если TSV не удалось, пробуем list формат
    if not keywords_available and not os.path.exists(keywords_path):
        for url in keywords_urls:
            logger.info(f"Пробуем скачать keywords list с {url}...")
            if download_file_with_progress(url, keywords_path_gz, "IMDB keywords list"):
                keywords_available = True
                break
        else:
            logger.warning("Не удалось скачать keywords ни с одного URL")
    elif os.path.exists(keywords_path):
        keywords_available = True
        logger.info("keywords.list уже существует")

    basics_tsv = os.path.join(DATA_DIR, 'title.basics.tsv')
    if not extract_gzip_with_progress(basics_path, basics_tsv, "basics"):
        raise Exception("Не удалось распаковать IMDB basics")
    
    if ratings_available:
        ratings_tsv = os.path.join(DATA_DIR, 'title.ratings.tsv')
        if not extract_gzip_with_progress(ratings_path, ratings_tsv, "ratings"):
            ratings_available = False
    
    if plots_available:
        plot_tsv = os.path.join(DATA_DIR, 'title.plot.summary.tsv')
        if not extract_gzip_with_progress(plot_path, plot_tsv, "plots"):
            plots_available = False
    
    # Распаковываем keywords если скачали
    if keywords_available and os.path.exists(keywords_path_gz) and not os.path.exists(keywords_path):
        if not extract_gzip_with_progress(keywords_path_gz, keywords_path, "keywords"):
            keywords_available = False
    
    return plots_available, ratings_available, keywords_available

def build_imdb_index():
    """Создаёт индекс IMDB фильмов (минимум 5000 оценок, все годы)"""
    if os.path.exists(INDEX_PATH) and os.path.exists(DATA_PATH):
        try:
            logger.info("Загружаем существующий индекс...")
            index = faiss.read_index(INDEX_PATH)
            movies = pd.read_csv(DATA_PATH)
            logger.info(f"Индекс загружен: {len(movies)} фильмов")
            return index, movies
        except Exception as e:
            logger.warning(f"Ошибка загрузки индекса: {e}, пересоздаём...")

    plots_available, ratings_available, keywords_available = download_imdb_data()

    logger.info("Загружаем IMDB данные...")
    basics = pd.read_csv(os.path.join(DATA_DIR, 'title.basics.tsv'), sep='\t', low_memory=False)

    # Фильтруем только фильмы (не сериалы для начала)
    movies = basics[basics['titleType'] == 'movie']
    movies = movies[['tconst', 'primaryTitle', 'startYear', 'genres']].copy()
    
    # Конвертируем startYear в числовой формат
    movies['startYear'] = pd.to_numeric(movies['startYear'], errors='coerce')
    movies = movies.dropna(subset=['startYear'])
    logger.info(f"Найдено {len(movies)} фильмов")
    
    # Объединяем с рейтингами для фильтрации по популярности
    if ratings_available and os.path.exists(os.path.join(DATA_DIR, 'title.ratings.tsv')):
        try:
            logger.info("Загружаем рейтинги для фильтрации по популярности...")
            ratings = pd.read_csv(os.path.join(DATA_DIR, 'title.ratings.tsv'), sep='\t', low_memory=False)
            movies = movies.merge(ratings[['tconst', 'averageRating', 'numVotes']], on='tconst', how='left')
            
            # Фильтруем только фильмы с минимум 5000 голосов
            MIN_VOTES = 5000
            movies_with_ratings = movies.dropna(subset=['numVotes'])
            movies_with_ratings = movies_with_ratings[movies_with_ratings['numVotes'] >= MIN_VOTES]
            
            if len(movies_with_ratings) > 0:
                logger.info(f"Найдено {len(movies_with_ratings)} популярных фильмов (минимум {MIN_VOTES} голосов)")
                movies = movies_with_ratings.sort_values('numVotes', ascending=False)
            else:
                logger.warning("Не найдено фильмов с достаточным количеством голосов")
        except Exception as e:
            logger.warning(f"Ошибка при загрузке рейтингов: {e}")
    else:
        logger.warning("Рейтинги недоступны")

    # Загружаем keywords (приоритет TSV формату)
    keywords_dict = {}
    if keywords_available:
        keywords_tsv_path = os.path.join(DATA_DIR, 'title.keywords.tsv')
        keywords_list_path = os.path.join(DATA_DIR, 'keywords.list')
        
        # Сначала пробуем TSV формат
        if os.path.exists(keywords_tsv_path):
            keywords_dict = parse_keywords_tsv(keywords_tsv_path)
            if keywords_dict:
                logger.info(f"Загружено ключевых слов для {len(keywords_dict)} фильмов из TSV")
        # Если TSV не доступен, пробуем list формат
        elif os.path.exists(keywords_list_path):
            keywords_dict = parse_keywords_list(keywords_list_path)
            if keywords_dict:
                logger.info(f"Загружено ключевых слов для {len(keywords_dict)} фильмов из list")
        
        if not keywords_dict:
            logger.warning("Не удалось загрузить keywords ни из одного формата")

    # Слияние с описаниями
    if plots_available and os.path.exists(os.path.join(DATA_DIR, 'title.plot.summary.tsv')):
        try:
            plots = pd.read_csv(os.path.join(DATA_DIR, 'title.plot.summary.tsv'), sep='\t', low_memory=False)
            movies = movies.merge(plots[['tconst', 'plot']], on='tconst', how='left')
            movies['description'] = movies['plot']
            movies = movies.dropna(subset=['plot'])
        except Exception as e:
            logger.warning(f"Ошибка при загрузке plots: {e}")
            plots_available = False
    
    if not plots_available or 'description' not in movies.columns:
        logger.info("Создаём описания из доступных данных...")
        movies['description'] = movies.apply(
            lambda row: f"{row['primaryTitle']} ({row['startYear'] if pd.notna(row['startYear']) else 'N/A'})" + 
                       (f" - {row['genres']}" if pd.notna(row.get('genres')) else ""),
            axis=1
        )
        movies = movies.dropna(subset=['description'])

    # Добавляем keywords к описаниям
    if keywords_dict:
        logger.info("Добавляем keywords к описаниям фильмов...")
        def add_keywords_to_description(row):
            tconst = row['tconst']
            description = row['description']
            
            if tconst in keywords_dict:
                keywords = keywords_dict[tconst]
                # Берем первые 10 keywords, чтобы не перегружать описание
                keywords_str = ', '.join(keywords[:10])
                description = f"{description} Keywords: {keywords_str}"
            
            return description
        
        movies['description'] = movies.apply(add_keywords_to_description, axis=1)
        logger.info("Keywords добавлены к описаниям")

    movies['title'] = movies['primaryTitle']
    movies['year'] = movies['startYear'].astype('Int64')
    movies['imdb_id'] = movies['tconst']

    logger.info(f"Создаём индекс для {len(movies)} фильмов...")
    model = get_model()
    embeddings = model.encode(movies['description'].tolist(), show_progress_bar=True)
    embeddings = np.array(embeddings).astype('float32')

    dimension = embeddings.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings)

    faiss.write_index(index, INDEX_PATH)
    movies[['imdb_id', 'title', 'year', 'description']].to_csv(DATA_PATH, index=False)
    logger.info("Индекс создан и сохранён")

    return index, movies

def get_index_and_movies():
    """Ленивая загрузка индекса и базы фильмов"""
    global _index, _movies
    if _index is None or _movies is None:
        _index, _movies = build_imdb_index()
    return _index, _movies

def get_whisper():
    """Ленивая загрузка модели Whisper (основной вариант для распознавания речи)"""
    global _whisper
    if _whisper is None:
        logger.info("Загрузка модели Whisper (основной вариант)...")
        try:
            from transformers import pipeline
            _whisper = pipeline("automatic-speech-recognition", model="openai/whisper-small", device=-1)
            logger.info("Whisper загружен успешно")
        except Exception as e:
            logger.warning(f"Не удалось загрузить Whisper: {e}")
            _whisper = False
    return _whisper if _whisper is not False else None

def get_vosk():
    """Ленивая загрузка модели Vosk (запасной вариант для распознавания речи)"""
    global _vosk
    if _vosk is None:
        logger.info("Загрузка модели Vosk (запасной вариант)...")
        try:
            import vosk
            import json
            
            # Используем легкую русско-английскую модель
            # Модель будет скачана автоматически при первом использовании
            model_name = "vosk-model-small-ru-0.22"  # Легкая модель для русского и английского
            
            # Путь к модели (в data/vosk или /data/vosk на Railway)
            model_path = os.path.join(DATA_DIR, '..', 'vosk', model_name)
            if not os.path.exists(model_path):
                # Пробуем альтернативный путь
                alt_model_path = os.path.join(BASE_DIR, 'data', 'vosk', model_name)
                if os.path.exists(alt_model_path):
                    model_path = alt_model_path
                else:
                    # Используем встроенную модель если доступна
                    logger.warning(f"Модель Vosk не найдена по пути {model_path}, пробуем использовать встроенную...")
                    model_path = None
            
            if model_path and os.path.exists(model_path):
                model = vosk.Model(model_path)
            else:
                # Пробуем использовать встроенную модель или скачать
                try:
                    # Vosk может работать с минимальной моделью
                    model = vosk.Model(lang="ru")  # Пробуем встроенную
                except:
                    # Если не получилось, возвращаем None
                    logger.warning("Не удалось загрузить модель Vosk")
                    _vosk = False
                    return None
            
            recognizer = vosk.KaldiRecognizer(model, 16000)  # 16kHz sample rate
            _vosk = {'model': model, 'recognizer': recognizer}
            logger.info("Vosk загружен успешно")
        except ImportError:
            logger.warning("Vosk не установлен. Установите: pip install vosk")
            _vosk = False
        except Exception as e:
            logger.warning(f"Не удалось загрузить Vosk: {e}")
            _vosk = False
    
    return _vosk if _vosk is not False else None

def transcribe_with_vosk(audio_path):
    """
    Распознает речь с помощью Vosk
    
    Args:
        audio_path: путь к WAV файлу (16kHz, mono)
    
    Returns:
        str: распознанный текст или None
    """
    try:
        vosk_data = get_vosk()
        if not vosk_data:
            return None
        
        import vosk
        import wave
        import json
        
        wf = wave.open(audio_path, "rb")
        
        # Проверяем формат
        if wf.getnchannels() != 1:
            logger.warning("Vosk требует mono WAV файл, конвертируем...")
            wf.close()
            # Конвертируем в mono если нужно
            from pydub import AudioSegment
            audio = AudioSegment.from_wav(audio_path)
            audio = audio.set_channels(1)
            mono_path = audio_path.replace('.wav', '_mono.wav')
            audio.export(mono_path, format="wav")
            wf = wave.open(mono_path, "rb")
            audio_path = mono_path
        
        # Создаем новый recognizer с правильной частотой
        sample_rate = wf.getframerate()
        model = vosk_data['model']
        recognizer = vosk.KaldiRecognizer(model, sample_rate)
        recognizer.SetWords(True)
        
        results = []
        while True:
            data = wf.readframes(4000)
            if len(data) == 0:
                break
            if recognizer.AcceptWaveform(data):
                result = json.loads(recognizer.Result())
                if 'text' in result and result['text']:
                    results.append(result['text'])
        
        # Получаем финальный результат
        final_result = json.loads(recognizer.FinalResult())
        if 'text' in final_result and final_result['text']:
            results.append(final_result['text'])
        
        wf.close()
        
        # Удаляем временный mono файл если создавали
        if audio_path.endswith('_mono.wav') and os.path.exists(audio_path):
            os.remove(audio_path)
        
        text = ' '.join(results).strip()
        return text if text else None
    except Exception as e:
        logger.error(f"Ошибка при распознавании с Vosk: {e}", exc_info=True)
        return None

def search_movies(query, top_k=5):
    """
    Поиск фильмов по запросу
    
    Args:
        query: текстовый запрос пользователя
        top_k: количество результатов (по умолчанию 5)
    
    Returns:
        list: список словарей с информацией о фильмах [{'imdb_id': 'tt123', 'title': '...', 'year': 2000}, ...]
    """
    try:
        query_en = translate_to_english(query)
        model = get_model()
        index, movies = get_index_and_movies()
        query_emb = model.encode([query_en])[0].astype('float32').reshape(1, -1)
        D, I = index.search(query_emb, k=top_k)
        
        results = []
        for idx in I[0]:
            row = movies.iloc[idx]
            results.append({
                'imdb_id': row['imdb_id'],
                'title': row['title'],
                'year': int(row['year']) if pd.notna(row['year']) else None,
                'description': row['description'][:200] if len(row['description']) > 200 else row['description']
            })
        
        return results
    except Exception as e:
        logger.error(f"Ошибка при поиске фильмов: {e}", exc_info=True)
        return []

