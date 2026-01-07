"""
Сервис для поиска фильмов по описанию (КиноШазам)
Использует TMDB датасет (оффлайн), semantic search, переводчик и распознавание речи
"""
import os
import logging
import pandas as pd
import numpy as np
import faiss
import json
import soundfile as sf
import librosa
from pathlib import Path
from sentence_transformers import SentenceTransformer
from transformers import pipeline
from pydub import AudioSegment
import torch
import gc
from tqdm import tqdm
from datetime import datetime

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

# Путь к TMDB датасету
TMDB_PARQUET_PATH = CACHE_DIR / 'tmdb_movies.parquet'

# Индексы и обработанные данные
INDEX_PATH = DATA_DIR / 'tmdb_index.faiss'
DATA_PATH = DATA_DIR / 'tmdb_movies_processed.csv'

# Настройки фильтрации
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
        logger.info("Загрузка транслятора русский → английский...")
        try:
            torch.set_num_threads(1)
            torch.set_grad_enabled(False)
            _translator = pipeline(
                "translation",
                model="Helsinki-NLP/opus-mt-ru-en",
                device=-1,
                torch_dtype=torch.float32
            )
            test_result = _translator("тест", max_length=512)
            logger.info(f"Транслятор загружен успешно (тест: 'тест' → '{test_result[0]['translation_text']}')")
        except Exception as e:
            logger.error(f"Ошибка загрузки транслятора: {e}", exc_info=True)
            _translator = False
    return _translator


def get_whisper():
    global _whisper
    if _whisper is None:
        logger.info(f"Загрузка модели Whisper (кэш: {CACHE_DIR})...")
        try:
            import whisper
            whisper_cache_dir = CACHE_DIR / 'whisper'
            whisper_cache_dir.mkdir(parents=True, exist_ok=True)
            whisper_model = whisper.load_model("base", download_root=str(whisper_cache_dir))
            
            class WhisperWrapper:
                def __init__(self, model):
                    self.model = model
                def __call__(self, audio_path):
                    result = self.model.transcribe(audio_path, language="ru")
                    return {"text": result.get("text", "").strip()}
            
            _whisper = WhisperWrapper(whisper_model)
            logger.info("Модель Whisper загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки Whisper: {e}", exc_info=True)
            _whisper = False
    return _whisper


def get_vosk():
    """Ленивая загрузка модели Vosk для распознавания речи (fallback)"""
    global _vosk
    if _vosk is None:
        logger.info("Загрузка модели Vosk...")
        try:
            import vosk
            vosk_model_path = DATA_DIR / 'vosk-model-small-ru-0.22'
            
            if not vosk_model_path.exists():
                logger.warning(f"Модель Vosk не найдена по пути {vosk_model_path}")
                logger.info("Скачайте модель с https://alphacephei.com/vosk/models и положите в data/shazam/")
                _vosk = False
                return _vosk
            
            model = vosk.Model(str(vosk_model_path))
            _vosk = vosk.KaldiRecognizer(model, 16000)
            _vosk.SetWords(True)
            logger.info("Модель Vosk загружена")
        except Exception as e:
            logger.error(f"Ошибка загрузки Vosk: {e}", exc_info=True)
            _vosk = False
    return _vosk


def translate_to_english(text):
    translator = get_translator()
    if not translator or translator is False:
        return text
    
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
    logger.info(f"[WHISPER] Начинаем распознавание: {audio_path}")
    whisper = get_whisper()
    if not whisper:
        return None
    try:
        result = whisper(audio_path)
        text = result.get("text", "").strip()
        if text:
            logger.info(f"[WHISPER] Распознано: '{text}'")
            return text
    except Exception as e:
        logger.error(f"[WHISPER] Ошибка: {e}", exc_info=True)
    return None


def transcribe_with_vosk(audio_path):
    """Распознает речь с помощью Vosk (fallback)"""
    vosk_recognizer = get_vosk()
    if not vosk_recognizer:
        return None
    
    try:
        data, sample_rate = sf.read(audio_path)
        
        if len(data.shape) > 1:
            data = np.mean(data, axis=1)
        
        if sample_rate != 16000:
            data = librosa.resample(data, orig_sr=sample_rate, target_sr=16000)
        
        data_int16 = (data * 32768).astype(np.int16)
        
        text_parts = []
        chunk_size = 4000
        recognizer = get_vosk()  # На всякий случай свежий
        
        for i in range(0, len(data_int16), chunk_size):
            chunk = data_int16[i:i+chunk_size].tobytes()
            if recognizer.AcceptWaveform(chunk):
                result = json.loads(recognizer.Result())
                if result.get('text'):
                    text_parts.append(result['text'])
        
        final_result = json.loads(recognizer.FinalResult())
        if final_result.get('text'):
            text_parts.append(final_result['text'])
        
        text = ' '.join(text_parts).strip()
        if text:
            logger.info(f"Vosk распознал: '{text}'")
            return text
        return None
    except Exception as e:
        logger.error(f"Ошибка распознавания Vosk: {e}", exc_info=True)
        return None


def transcribe_voice(audio_path):
    """Распознает речь из аудио файла (Whisper → Vosk)"""
    logger.info(f"[TRANSCRIBE] Начинаем распознавание: {audio_path}")
    
    # Сначала Whisper
    text = transcribe_with_whisper(audio_path)
    if text:
        return text
    
    logger.info(f"[TRANSCRIBE] Whisper не распознал, пробуем Vosk...")
    text = transcribe_with_vosk(audio_path)
    if text:
        return text
    
    logger.warning(f"[TRANSCRIBE] Ни Whisper, ни Vosk не смогли распознать речь")
    return None


def convert_ogg_to_wav(ogg_path, wav_path, sample_rate=16000):
    try:
        audio = AudioSegment.from_ogg(ogg_path)
        audio = audio.set_frame_rate(sample_rate).set_channels(1)
        audio.export(wav_path, format="wav")
        return True
    except Exception as e:
        logger.error(f"Ошибка конвертации OGG в WAV: {e}", exc_info=True)
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
            logger.info(f"Загружен существующий индекс: {len(_movies_df)} фильмов")
            return _index, _movies_df
        except Exception as e:
            logger.warning(f"Ошибка загрузки индекса: {e}, пересоздаем...")
    
    # Автоматическое скачивание TMDB если нет
    if not TMDB_PARQUET_PATH.exists():
        logger.info("TMDB parquet не найден — скачиваем с Kaggle...")
        try:
            import kagglehub
            import zipfile
            import glob
            import shutil
            
            # Скачиваем весь датасет (он в zip)
            dataset_path = kagglehub.dataset_download("asaniczka/tmdb-movies-dataset-2023-930k-movies")
            logger.info(f"Датасет скачан в: {dataset_path}")
            
            # Находим zip файл(ы)
            zip_files = glob.glob(os.path.join(dataset_path, "*.zip"))
            if not zip_files:
                raise Exception("Zip файл не найден в датасете")
            
            # Распаковываем первый zip
            with zipfile.ZipFile(zip_files[0], 'r') as zip_ref:
                zip_ref.extractall(CACHE_DIR)
            
            # Находим parquet и перемещаем/переименовываем
            parquet_files = glob.glob(os.path.join(CACHE_DIR, "**/*.parquet"), recursive=True)
            if not parquet_files:
                raise Exception("Parquet не найден после распаковки")
            
            main_parquet = parquet_files[0]
            shutil.move(main_parquet, TMDB_PARQUET_PATH)
            logger.info(f"TMDB parquet готов: {TMDB_PARQUET_PATH}")
            
        except Exception as e:
            logger.error(f"Ошибка скачивания TMDB: {e}")
            return None, None
    
    # Vosk модель (опционально, если используешь fallback)
    vosk_model_dir = DATA_DIR / 'vosk-model-small-ru-0.22'
    if not vosk_model_dir.exists():
        logger.info("Vosk модель не найдена — скачиваем...")
        try:
            import requests
            from io import BytesIO
            from zipfile import ZipFile
            
            url = "https://alphacephei.com/vosk/models/vosk-model-small-ru-0.22.zip"
            r = requests.get(url)
            r.raise_for_status()
            
            with ZipFile(BytesIO(r.content)) as z:
                z.extractall(DATA_DIR)
            
            logger.info(f"Vosk модель готова: {vosk_model_dir}")
        except Exception as e:
            logger.warning(f"Не удалось скачать Vosk: {e} — fallback не будет работать")
            
    logger.info("Загружаем TMDB датасет...")
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
    
    logger.info(f"Создаём индекс для {len(processed)} фильмов...")
    
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
    
    logger.info(f"Индекс TMDB создан: {len(processed)} фильмов")
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
        logger.error(f"Ошибка поиска: {e}", exc_info=True)
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

