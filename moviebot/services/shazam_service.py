"""
Сервис для поиска фильмов по описанию (КиноШазам)
Использует TMDB датасет (оффлайн), semantic search, переводчик и распознавание речи
+ OMDB для постеров/рейтинга + IMDb-база для актёров/режиссёров
"""
import os
import logging
import pandas as pd
import numpy as np
import faiss
import json
import soundfile as sf
import librosa
import sqlite3
import requests
import subprocess
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

# Пути
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'shazam'
DATA_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path('cache')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

os.environ['HF_HOME'] = str(CACHE_DIR / 'huggingface')
os.environ['TRANSFORMERS_CACHE'] = str(CACHE_DIR / 'huggingface' / 'transformers')
os.environ['SENTENCE_TRANSFORMERS_HOME'] = str(CACHE_DIR / 'huggingface' / 'sentence_transformers')

TMDB_CSV_PATH = CACHE_DIR / 'TMDB_movie_dataset_v11.csv'
INDEX_PATH = DATA_DIR / 'tmdb_index.faiss'
DATA_PATH = DATA_DIR / 'tmdb_movies_processed.csv'

# IMDb пути
IMDB_RAW_DIR = BASE_DIR / 'data' / 'imdb_raw'
IMDB_RAW_DIR.mkdir(parents=True, exist_ok=True)
IMDB_DB_PATH = BASE_DIR / 'data' / 'imdb_minimal.db'

# Настройки TMDB
MIN_VOTE_COUNT = 500
MAX_MOVIES = 50000

OMDB_API_KEY = os.getenv("OMDB_API_KEY")


def build_imdb_database():
    """Скачивает и строит минимальную IMDb-базу один раз, если её нет"""
    if IMDB_DB_PATH.exists():
        logger.info("IMDb-база уже существует, пропускаем построение")
        return

    logger.info("IMDb-база отсутствует — начинаем скачивание и построение")

    files = {
        'title.basics.tsv.gz': 'https://datasets.imdbws.com/title.basics.tsv.gz',
        'title.crew.tsv.gz': 'https://datasets.imdbws.com/title.crew.tsv.gz',
        'title.principals.tsv.gz': 'https://datasets.imdbws.com/title.principals.tsv.gz',
        'name.basics.tsv.gz': 'https://datasets.imdbws.com/name.basics.tsv.gz'
    }

    for fname, url in files.items():
        path = IMDB_RAW_DIR / fname
        if not path.exists():
            logger.info(f"Скачиваем {fname}...")
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"{fname} скачан")
            except Exception as e:
                logger.error(f"Ошибка скачивания {fname}: {e}")
                return

    # Загрузка и фильтрация
    def load_tsv(file_name):
        path = IMDB_RAW_DIR / file_name
        return pd.read_csv(path, sep='\t', low_memory=False, na_values='\\N')

    logger.info("Загружаем basics...")
    basics = load_tsv('title.basics.tsv.gz')
    movies = basics[basics['titleType'] == 'movie']
    movies_tconst = set(movies['tconst'])

    logger.info(f"Фильмов: {len(movies)}")

    logger.info("Загружаем crew...")
    crew = load_tsv('title.crew.tsv.gz')
    crew = crew[crew['tconst'].isin(movies_tconst)]

    logger.info("Загружаем principals...")
    principals = load_tsv('title.principals.tsv.gz')
    principals = principals[principals['tconst'].isin(movies_tconst)]

    logger.info("Загружаем names...")
    used_nconst = set(principals['nconst'].dropna())
    used_nconst.update(crew['directors'].str.cat(sep=',').split(','))
    used_nconst.update(crew['writers'].str.cat(sep=',').split(','))
    used_nconst = {x for x in used_nconst if pd.notna(x) and x}
    names = load_tsv('name.basics.tsv.gz')
    names = names[names['nconst'].isin(used_nconst)]

    # Создаём базу
    conn = sqlite3.connect(IMDB_DB_PATH)

    logger.info("Записываем таблицы...")
    movies[['tconst', 'primaryTitle', 'originalTitle', 'startYear']].to_sql('movies', conn, if_exists='replace', index=False)
    crew.to_sql('crew', conn, if_exists='replace', index=False)
    principals[['tconst', 'ordering', 'nconst', 'category']].to_sql('principals', conn, if_exists='replace', index=False)
    names[['nconst', 'primaryName']].to_sql('names', conn, if_exists='replace', index=False)

    conn.close()
    logger.info(f"IMDb-база создана: {IMDB_DB_PATH} ({IMDB_DB_PATH.stat().st_size / 1e9:.2f} GB)")

    # Удаляем gz-файлы, чтобы освободить место
    for f in IMDB_RAW_DIR.glob('*.gz'):
        f.unlink()
        logger.info(f"Удалён {f}")


def get_imdb_enrich(imdb_id):
    """Возвращает актёров и режиссёров из локальной IMDb-базы"""
    if not IMDB_DB_PATH.exists():
        logger.warning("IMDb-база не найдена, пропускаем enrich")
        return {}

    if not imdb_id.startswith('tt'):
        imdb_id = f"tt{imdb_id.zfill(7)}"

    conn = sqlite3.connect(IMDB_DB_PATH)
    cur = conn.cursor()

    # Топ-10 актёров
    cur.execute("""
        SELECT GROUP_CONCAT(n.primaryName, ', ')
        FROM principals p
        JOIN names n ON p.nconst = n.nconst
        WHERE p.tconst = ? AND p.category IN ('actor', 'actress')
        ORDER BY p.ordering LIMIT 10
    """, (imdb_id,))
    actors = cur.fetchone()[0] or ''

    # Режиссёры (может быть несколько)
    cur.execute("""
        SELECT GROUP_CONCAT(DISTINCT n.primaryName, ', ')
        FROM crew c
        JOIN names n ON ',' || c.directors || ',' LIKE '%,' || n.nconst || ',%'
        WHERE c.tconst = ?
    """, (imdb_id,))
    directors = cur.fetchone()[0] or ''

    conn.close()

    return {
        'actors': actors,
        'directors': directors
    }


def get_omdb_details(imdb_id: str) -> dict:
    if not imdb_id or not imdb_id.startswith("tt") or not OMDB_API_KEY:
        return {}

    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_API_KEY}"
    try:
        response = requests.get(url, timeout=7)
        data = response.json()
        if data.get("Response") == "True":
            return {
                "title": data.get("Title", ""),
                "year": data.get("Year", ""),
                "director": data.get("Director", "Не указано"),
                "actors": data.get("Actors", "Не указано"),
                "imdb_rating": data.get("imdbRating", "N/A"),
                "poster_url": data.get("Poster", None),
                "runtime": data.get("Runtime", ""),
                "genre": data.get("Genre", ""),
                "plot": data.get("Plot", ""),
            }
    except Exception as e:
        logger.warning(f"OMDB ошибка для {imdb_id}: {e}")
    return {}


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
                dtype=torch.float32
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
    global _vosk
    if _vosk is None:
        logger.info("Загрузка модели Vosk...")
        try:
            import vosk
            vosk_model_path = DATA_DIR / 'vosk-model-small-ru-0.22'
            
            if not vosk_model_path.exists():
                logger.warning(f"Модель Vosk не найдена по пути {vosk_model_path}")
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
        recognizer = get_vosk()
        
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
    logger.info(f"[TRANSCRIBE] Начинаем распознавание: {audio_path}")
    text = transcribe_with_whisper(audio_path)
    if text:
        return text
    logger.info(f"[TRANSCRIBE] Whisper не распознал, пробуем Vosk...")
    text = transcribe_with_vosk(audio_path)
    if text:
        return text
    logger.warning(f"[TRANSCRIBE] Ни один метод не распознал речь")
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
            logger.warning(f"Ошибка загрузки старого индекса: {e}, пересоздаем...")

    if not TMDB_CSV_PATH.exists():
        logger.info("TMDB CSV не найден — скачиваем через Kaggle API...")
        try:
            import subprocess
            
            kaggle_dir = Path("/root/.kaggle")
            kaggle_dir.mkdir(parents=True, exist_ok=True)
            kaggle_json = kaggle_dir / "kaggle.json"
            if not kaggle_json.exists():
                with open(kaggle_json, "w") as f:
                    f.write(f'{{"username":"{os.getenv("KAGGLE_USERNAME")}","key":"{os.getenv("KAGGLE_KEY")}"}}')
                os.chmod(kaggle_json, 0o600)
            
            subprocess.check_call([
                "kaggle", "datasets", "download", "-d", "asaniczka/tmdb-movies-dataset-2023-930k-movies",
                "-p", str(CACHE_DIR), "--unzip"
            ])
            
            possible_files = list(CACHE_DIR.glob("TMDB_movie_dataset_v*.csv"))
            if not possible_files:
                raise Exception("CSV файл TMDB_movie_dataset_v*.csv не найден после распаковки")
            
            actual_csv = possible_files[0]
            if actual_csv != TMDB_CSV_PATH:
                actual_csv.rename(TMDB_CSV_PATH)
            
            logger.info(f"TMDB CSV готов: {TMDB_CSV_PATH}")
            
        except Exception as e:
            logger.error(f"Ошибка скачивания TMDB: {e}", exc_info=True)
            raise

    logger.info("Загружаем TMDB датасет из CSV...")
    df = pd.read_csv(TMDB_CSV_PATH)
    logger.info(f"Загружено {len(df)} записей")

    df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    current_year = datetime.now().year
    df = df[(df['year'] >= current_year - 50) & (df['vote_count'] >= MIN_VOTE_COUNT)]
    df = df.dropna(subset=['overview', 'title'])
    df = df.sort_values('vote_count', ascending=False).head(MAX_MOVIES * 2)

    df['genres_str'] = df['genres'].apply(lambda x: parse_json_list(x, 'name'))
    df['keywords_str'] = df['keywords'].apply(lambda x: parse_json_list(x, 'name', top_n=15))

    df['description'] = df.apply(
        lambda row: f"{row['title']} ({row['year']}). "
                    f"Original title: {row.get('original_title', '')}. "
                    f"Tagline: {row.get('tagline', '')}. "
                    f"Genres: {row['genres_str']}. "
                    f"Plot: {row['overview']}. "
                    f"Keywords: {row['keywords_str']}.",
        axis=1
    )

    df['imdb_id'] = df['imdb_id'].astype(str).str.replace('tt', '', regex=False)
    processed = df[['imdb_id', 'title', 'year', 'description']].copy()
    processed = processed.head(MAX_MOVIES)

    logger.info(f"Создаём эмбеддинги для {len(processed)} фильмов...")
    model = get_model()
    descriptions = processed['description'].tolist()

    embeddings = []
    batch_size = 32
    for i in tqdm(range(0, len(descriptions), batch_size), desc="Генерация эмбеддингов"):
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

    logger.info(f"Индекс TMDB успешно создан: {len(processed)} фильмов")
    return index, processed


def get_index_and_movies():
    global _index, _movies_df
    if _index is None or _movies_df is None:
        _index, _movies_df = build_tmdb_index()
    return _index, _movies_df


def search_movies(query, top_k=5):
    """
    Основная функция поиска.
    Теперь возвращает список словарей с данными из OMDB + IMDb (актёры/режиссёры)
    """
    try:
        query_en = translate_to_english(query)
        index, movies = get_index_and_movies()
        if index is None:
            return []
        
        model = get_model()
        query_emb = model.encode([query_en])[0].astype('float32').reshape(1, -1)
        
        D, I = index.search(query_emb, k=top_k * 2)
        
        results = []
        for idx in I[0]:
            if len(results) >= top_k:
                break
            if idx >= len(movies):
                continue
                
            row = movies.iloc[idx]
            imdb_id_raw = row['imdb_id']
            if pd.isna(imdb_id_raw) or imdb_id_raw == 'nan':
                continue
            imdb_id = f"tt{str(imdb_id_raw).zfill(7)}"
            
            # OMDB
            omdb = get_omdb_details(imdb_id)
            
            # IMDb (актёры/режиссёры)
            imdb_enrich = get_imdb_enrich(imdb_id.replace('tt', ''))
            
            result = {
                'imdb_id': imdb_id.replace('tt', ''),
                'title': omdb.get('title') or row['title'],
                'year': omdb.get('year') or (int(row['year']) if pd.notna(row['year']) else None),
                'description': row['description'][:500],
                'director': omdb.get('director', imdb_enrich.get('directors', '')),
                'actors': omdb.get('actors', imdb_enrich.get('actors', '')),
                'imdb_rating': omdb.get('imdb_rating', 'N/A'),
                'poster_url': omdb.get('poster_url') if omdb.get('poster_url') not in ("N/A", None) else None,
                'runtime': omdb.get('runtime', ''),
                'genre': omdb.get('genre', ''),
            }
            results.append(result)
        
        return results
    
    except Exception as e:
        logger.error(f"Ошибка поиска: {e}", exc_info=True)
        return []