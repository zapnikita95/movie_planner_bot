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
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'shazam')
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_PATH = os.path.join(DATA_DIR, 'imdb_index.faiss')
DATA_PATH = os.path.join(DATA_DIR, 'imdb_movies.csv')

# Глобальные переменные для моделей (lazy loading)
_model = None
_translator = None
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

def download_imdb_data():
    basics_url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    ratings_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    plot_url = 'https://datasets.imdbws.com/title.plot.summary.tsv.gz'

    basics_path = os.path.join(DATA_DIR, 'title.basics.tsv.gz')
    ratings_path = os.path.join(DATA_DIR, 'title.ratings.tsv.gz')
    plot_path = os.path.join(DATA_DIR, 'title.plot.summary.tsv.gz')

    if not download_file_with_progress(basics_url, basics_path, "IMDB basics"):
        raise Exception("Не удалось скачать IMDB basics")
    
    ratings_available = download_file_with_progress(ratings_url, ratings_path, "IMDB ratings")
    if not ratings_available:
        logger.warning("IMDB ratings недоступен")
    
    plots_available = download_file_with_progress(plot_url, plot_path, "IMDB plots")
    if not plots_available:
        logger.warning("IMDB plots недоступен")

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
    
    return plots_available, ratings_available

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

    plots_available, ratings_available = download_imdb_data()

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
    """Ленивая загрузка модели распознавания речи (для fallback)"""
    # В основном используем Telegram API, но оставляем Whisper как fallback
    try:
        from transformers import pipeline
        return pipeline("automatic-speech-recognition", model="openai/whisper-small", device=-1)
    except:
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

