"""
Сервис для поиска фильмов по описанию (КиноШазам)
Использует TMDB датасет (оффлайн), semantic search, переводчик и whisper-tiny
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

# Пути — важно для Railway volume!
DATA_DIR = Path('/app/data/shazam')
DATA_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path('/app/cache')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Кэш для huggingface моделей
os.environ['HF_HOME'] = str(CACHE_DIR / 'huggingface')
os.environ['TRANSFORMERS_CACHE'] = str(CACHE_DIR / 'huggingface' / 'transformers')
os.environ['SENTENCE_TRANSFORMERS_HOME'] = str(CACHE_DIR / 'huggingface' / 'sentence_transformers')

TMDB_PARQUET_PATH = CACHE_DIR / 'tmdb_movies.parquet'
INDEX_PATH = DATA_DIR / 'tmdb_index.faiss'
DATA_PATH = DATA_DIR / 'tmdb_movies_processed.csv'

MIN_VOTE_COUNT = 500
MAX_MOVIES = 50000


def get_model():
    global _model
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
        logger.info(f"Загрузка whisper-tiny (кэш: {CACHE_DIR})...")
        try:
            whisper_cache = CACHE_DIR / 'whisper'
            whisper_cache.mkdir(parents=True, exist_ok=True)
            
            # Самое важное — tiny вместо base!
            model = whisper.load_model("tiny", download_root=str(whisper_cache))
            
            class WhisperWrapper:
                def __init__(self, model):
                    self.model = model
                    
                def __call__(self, audio_path):
                    result = self.model.transcribe(str(audio_path), language="ru")
                    return {"text": result.get("text", "").strip()}
            
            _whisper = WhisperWrapper(model)
            logger.info("whisper-tiny успешно загружен")
        except Exception as e:
            logger.error(f"Ошибка загрузки whisper-tiny: {e}", exc_info=True)
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
    """Whisper-tiny — единственный вариант распознавания"""
    logger.info(f"[TRANSCRIBE] Файл: {audio_path}")
    
    whisper_model = get_whisper()
    if not whisper_model:
        logger.error("Whisper-tiny не загрузился")
        return None
        
    try:
        result = whisper_model(audio_path)
        text = result.get("text", "").strip()
        if text:
            logger.info(f"[WHISPER-tiny] Распознано: {text[:120]}...")
            return text
        logger.warning("[WHISPER] Пустой результат")
    except Exception as e:
        logger.error(f"Whisper-tiny ошибка: {e}", exc_info=True)
    
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
    
    if INDEX_PATH.exists() and DATA_PATH.exists():
        try:
            _index = faiss.read_index(str(INDEX_PATH))
            _movies_df = pd.read_csv(DATA_PATH)
            logger.info(f"Индекс загружен: {len(_movies_df)} фильмов")
            return _index, _movies_df
        except Exception as e:
            logger.warning(f"Не удалось загрузить индекс: {e} → пересоздаём")
    
    # =============================================================================
    # Важно! Этот блок должен выполняться ТОЛЬКО один раз локально!
    # После создания файлов закомментируй/удали скачивание
    # =============================================================================
    if not TMDB_PARQUET_PATH.exists():
        logger.error(
            "TMDB parquet отсутствует!\n"
            "1. Запусти этот код ЛОКАЛЬНО один раз\n"
            "2. Скачай датасет вручную/через kaggle\n"
            "3. Положи в /app/cache/tmdb_movies.parquet\n"
            "4. Сгенерируй индекс и закоммить/залей в volume"
        )
        return None, None

    # Дальше — обработка датасета (оставляем как было)
    logger.info("Читаем TMDB датасет...")
    df = pd.read_parquet(TMDB_PARQUET_PATH)
    logger.info(f"Загружено {len(df)} записей")
    
    df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    current_year = datetime.now().year
    df = df[(df['year'] >= current_year - 50) & (df['vote_count'] >= MIN_VOTE_COUNT)]
    df = df.dropna(subset=['overview', 'title'])
    df = df.sort_values('vote_count', ascending=False).head(MAX_MOVIES * 2)
    
    df['genres_str'] = df['genres'].apply(lambda x: parse_json_list(x, 'name'))
    df['keywords_str'] = df['keywords'].apply(lambda x: parse_json_list(x, 'name', top_n=15))
    df['actors_str'] = df['cast'].apply(lambda x: parse_json_list(x, 'name', top_n=10))
    df['director_str'] = df['crew'].apply(lambda x: parse_json_list(x, 'name', top_n=3) if pd.notna(x) else '')
    
    df['description'] = df.apply(
        lambda row: f"{row['title']} ({row['year']}) {row['genres_str']}. "
                    f"Plot: {row['overview']}. "
                    f"Keywords: {row['keywords_str']}. "
                    f"Actors: {row['actors_str']}. "
                    f"Director: {row['director_str']}",
        axis=1
    )
    
    df['imdb_id'] = df['imdb_id'].astype(str).str.replace('tt', '', regex=False)
    processed = df[['imdb_id', 'title', 'year', 'description']].copy()
    processed = processed.head(MAX_MOVIES)
    
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
    if _index is None or _movies_df is None:
        _index, _movies_df = build_tmdb_index()
    return _index, _movies_df


def search_movies(query, top_k=5):
    try:
        query_en = translate_to_english(query)
        index, movies = get_index_and_movies()
        if index is None:
            return []
        
        model = get_model()
        query_emb = model.encode([query_en])[0].astype('float32').reshape(1, -1)
        
        D, I = index.search(query_emb, k=top_k)
        
        results = []
        for idx in I[0]:
            if idx < len(movies):
                row = movies.iloc[idx]
                results.append({
                    'imdb_id': row['imdb_id'],
                    'title': row['title'],
                    'year': row['year'] if pd.notna(row['year']) else None,
                    'description': row['description'][:500]
                })
        return results
    except Exception as e:
        logger.error(f"Ошибка поиска фильмов: {e}", exc_info=True)
        return []