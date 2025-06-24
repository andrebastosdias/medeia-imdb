import argparse
import ast
import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import TypeVar, cast

import pandas as pd
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset

import imdb


PandasLike = TypeVar('PandasLike', pd.DataFrame, pd.Series)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATASET_ROOT = "medeia"
DATASET_BASE = DATASET_ROOT + "/base"
DATASET_FILMS = DATASET_ROOT + "/films"

MEDEIA_URL = "https://medeiafilmes.com/filmes-em-exibicao"
DATA_PATTERN = re.compile(r"global\.data\s*=\s*(\{.*?\});")
TARGET_THEATERS = ["cinema-medeia-nimas"]
TIMEZEONE = "Europe/Lisbon"

async def handle(ctx: BeautifulSoupCrawlingContext) -> None:
    html = html = imdb.get_response(ctx.http_response)
    hit = DATA_PATTERN.search(html)
    if not hit:
        ctx.log.error("No global.data on %s", ctx.request.url)
        return

    data = json.loads(hit.group(1))
    await ctx.push_data({
        "url": ctx.request.url,
        "data": data,
    }, dataset_name=DATASET_BASE if ctx.request.crawl_depth == 0 else DATASET_FILMS)

    if ctx.request.crawl_depth == 0:
        movies = data["schedule"]["events"]
        await ctx.enqueue_links(requests=[
            movie_info['url'] for movie_id, movie_info in movies.items() if movie_id.startswith('film-')
        ])

def extract_film_data(data: dict) -> dict | None:
    film_data = data["data"]["film"]

    def extract_runtime() -> int:
        length = film_data["length"]
        numbers = list(map(int, re.findall(r'\d+', length)))
        if len(numbers) == 2:
            if '+' in length:
                hours, minutes = 0, numbers[0] + numbers[1]
            else:
                hours, minutes = numbers[0], numbers[1]
        elif len(numbers) == 1:
            hours, minutes = 0, numbers[0]
        else:
            raise ValueError(f"Unexpected duration format: {length}")
        return hours * 60 + minutes

    def extract_sessions() -> list[datetime]:
        sessions: list[datetime] = []
        for theater in film_data["programme"].values():
            if theater.get("slug") in TARGET_THEATERS:
                for session in theater["sessions"].values():
                    date = session["date"]
                    hours = session["hours"]
                    assert len(hours) == 1
                    hours = str(hours[0]).replace("*", "").strip()
                    if date and hours:
                        sessions.append(datetime.strptime(f"{date} {hours}", "%Y-%m-%d %H:%M"))
        return sorted(sessions)

    return {
        "id": int(film_data["id"]),
        "original_title": film_data["title_original"].strip(),
        "title": film_data["title"].strip(),
        "director": film_data["director_name"].strip(),
        "cast": [cast.strip() for cast in film_data["cast"].split(",")],
        "releaseYear": int(film_data["production_year"]),
        "runtime": extract_runtime(),
        "sessions": extract_sessions(),
        "url": data["url"],
    }

# def upsert_movies(df_db: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
#     logging.info(f"🟡 Upsert started: {len(df_new)} new records, {len(df_db)} in DB")

#     # Split by ID presence
#     in_db_mask = df_new.index.isin(df_db.index)
#     new_rows = df_new[~in_db_mask]
#     update_rows = df_new[in_db_mask]

#     logging.info(f"➕ New: {len(new_rows)} | 🔄 Update: {len(update_rows)}")

#     if not update_rows.empty:
#         check_cols = df_db.columns.difference(['sessions']).tolist()
#         cmp = (
#             update_rows[check_cols]
#             .compare(df_db.loc[update_rows.index, check_cols], keep_shape=False, keep_equal=False)
#         )
#         if not cmp.empty:
#             mismatch_summary = (
#                 cmp.stack(future_stack=True).reset_index()
#                 .rename(columns={'level_2': 'column', 0: 'new', 1: 'db'})
#             )
#             logging.error("❌ Conflicting metadata for existing IDs:\n%s", mismatch_summary.to_string(index=True))
#             raise ValueError("Conflicting metadata found in update_rows")

#         df_db.loc[update_rows.index, 'sessions'] = update_rows['sessions']
#         logging.info(f"✅ Updated sessions for {len(update_rows)} movies")

#     # Clear sessions for IDs no longer in new data
#     gone_mask = ~df_db.index.isin(df_new.index)
#     if gone_mask.any():
#         df_db.loc[gone_mask, 'sessions'] = pd.NA
#     logging.info(f"🗑️  Cleared sessions for {gone_mask.sum()} movies no longer listed")

#     # Append brand new rows
#     df_medeia = pd.concat([df_db, new_rows])

#     logging.info(f"✅ Upsert complete: {len(df_medeia)} total records")

#     df_medeia.sort_index(inplace=True)

#     return df_medeia

def match_series_imdb(movie_row: pd.Series, df_imdb: pd.DataFrame):
    original_movie_row = movie_row.copy()
    original_df_imdb = df_imdb.copy()

    def convert_to_ascii(df: PandasLike) -> PandasLike:
        if isinstance(df, pd.Series):
            df['title'] = imdb.to_ascii(df['title'])
            df['original_title'] = imdb.to_ascii(df['original_title'])
            df['director'] = imdb.to_ascii(df['director'])
            df['cast'] = [imdb.to_ascii(actor) for actor in df['cast']]
        else:
            df['title'] = df['title'].apply(imdb.to_ascii)
            df['original_title'] = df['original_title'].apply(imdb.to_ascii)
            df['directors'] = df['directors'].apply(lambda directors: [imdb.to_ascii(director) for director in directors])
            df['cast'] = df['cast'].apply(lambda cast: [imdb.to_ascii(actor) for actor in cast])
        return df

    movie_row = convert_to_ascii(movie_row)
    df_imdb = convert_to_ascii(df_imdb)

    df_matches = pd.DataFrame(index=df_imdb.index)
    df_matches['title'] = (
        (df_imdb['title'] == movie_row['title']) |
        (df_imdb['title'] == movie_row['original_title']) |
        (df_imdb['original_title'] == movie_row['title']) |
        (df_imdb['original_title'] == movie_row['original_title'])
    )
    df_matches['directors'] = df_imdb['directors'].apply(lambda directors: movie_row['director'] in directors)
    df_matches['cast'] = df_imdb['cast'].apply(lambda cast: bool(set(movie_row['cast']).intersection(cast)))
    df_matches['releaseYear'] = df_imdb['releaseYear'] == movie_row['releaseYear']
    df_matches['runtime'] = (
        df_imdb['runtime'].notna() & (df_imdb['runtime'] == movie_row['runtime'] * 60)
    )

    df_matches = df_matches[df_matches.sum(axis=1) >= 3]
    assert df_matches.shape[0] <= 1, f"Multiple matches found for movie: {movie_row['title']}"

    if df_matches.shape[0] == 1:
        match = cast(pd.Series, original_df_imdb.loc[df_matches.index[0]])
        original_movie_row['watched'] = match['watched']
        original_movie_row['imdb_id'] = match.name
        original_movie_row['imdb_lists'] = match['imdb_lists']
    else:
        original_movie_row['watched'] = False
        original_movie_row['imdb_id'] = pd.NA
        original_movie_row['imdb_lists'] = []

    original_movie_row['url'] = original_movie_row.pop('url')

    return original_movie_row

async def get_medeia_movies() -> pd.DataFrame:
    df = await Dataset.open(name=DATASET_ROOT)
    await df.drop()
    crawler = BeautifulSoupCrawler(max_crawl_depth=1)
    crawler.router.default_handler(handle)
    await crawler.run([MEDEIA_URL])

    ds = await Dataset.open(name=DATASET_FILMS)
    content = await ds.get_data()
    rows = [extract_film_data(item) for item in content.items]
    df_medeia = pd.DataFrame.from_records(rows, index='id')

    return df_medeia

async def get_imdb_movies(user_id: str) -> pd.DataFrame:
    imdb_tuple = await imdb.get_lists(user_id)
    df_imdb_watchlist = pd.DataFrame.from_records(imdb_tuple[0], index='id')
    df_imdb_lists = {name: pd.DataFrame.from_records(items, index='id') for name, items in imdb_tuple[1].items()}

    df_imdb_watchlist['watched'] = True
    df_imdb_watchlist['imdb_lists'] = [[] for _ in range(len(df_imdb_watchlist))]
    columns = df_imdb_watchlist.columns

    for name, df_imdb_list in df_imdb_lists.items():
        df_imdb_list['imdb_lists_dup'] = [[name] for _ in range(len(df_imdb_list))]
        df_imdb_watchlist = df_imdb_watchlist.combine_first(df_imdb_list)
        df_imdb_watchlist['imdb_lists'] = df_imdb_watchlist['imdb_lists'].combine(
            df_imdb_watchlist['imdb_lists_dup'], lambda x, y: (x if isinstance(x, list) else []) + (y if isinstance(y, list) else [])
        )
        df_imdb_watchlist.drop('imdb_lists_dup', axis=1, inplace=True)

    df_imdb_watchlist = df_imdb_watchlist.convert_dtypes()
    df_imdb_watchlist['watched'] = df_imdb_watchlist['watched'].fillna(False)
    df_imdb_watchlist = df_imdb_watchlist.reindex(columns, axis=1)
    df_imdb_watchlist.sort_index(inplace=True)
    return df_imdb_watchlist

def get_sessions(df_movies: pd.DataFrame) -> pd.DataFrame:
    df_sessions = df_movies.dropna(subset=['sessions']).explode('sessions')
    df_sessions.rename(columns={'sessions': 'session'}, inplace=True)
    df_sessions.dropna(subset=['session'], inplace=True)

    df_sessions['session'] = pd.to_datetime(df_sessions['session']).dt.tz_localize(TIMEZEONE)
    df_sessions = df_sessions[df_sessions['session'] >= pd.Timestamp.now(tz=TIMEZEONE).normalize()]

    session_position: int = cast(int, df_sessions.columns.get_loc('session'))

    df_sessions.insert(
        session_position + 1,
        'weekday',
        df_sessions['session'].dt.day_name()
    )

    df_sessions.insert(
        session_position + 2,
        'at_work',
        (df_sessions['session'].dt.weekday < 5) & (df_sessions['session'].dt.hour < 17)
    )

    df_sessions.insert(
        session_position + 3,
        'sessions_left',
        (~df_sessions['at_work']).groupby(df_sessions.index).transform(lambda x: x[::-1].cumsum()[::-1])
    )

    df_sessions.sort_values(by='session', inplace=True)
    df_sessions.reset_index(inplace=True)
    return df_sessions

async def main(user_id: str, reload: bool = True):
    file_path = DATA_DIR / f"movies.csv"

    df_movies = pd.read_csv(
        file_path,
        index_col='id',
        converters={
            "cast": ast.literal_eval,
            "imdb_lists": ast.literal_eval,
        },
        encoding='utf-8-sig'
    ) if os.path.exists(file_path) else None

    if not reload and df_movies is not None:
        df_medeia = df_movies
        logging.info(f"📂 Loaded {len(df_medeia)} movies from database")
    else:
        df_medeia = await get_medeia_movies()
        if df_movies is None:
            logging.info(f"📂 Loaded {len(df_medeia)} movies from Medeia website")
        else:
            columns = df_medeia.columns
            df_medeia = df_medeia.combine_first(df_movies)[columns]

    df_imdb = await get_imdb_movies(user_id)

    df_movies = df_medeia.apply(
        match_series_imdb, axis=1, args=(df_imdb,), result_type='expand'
    )
    df_movies.to_csv(DATA_DIR / "movies.csv", index=True, encoding='utf-8-sig')

    df_sessions = get_sessions(df_movies)
    df_sessions.to_csv(DATA_DIR / "sessions.csv", index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the main async process.")
    parser.add_argument("--user-id", "-u", type=str, required=True, help="The user_id to process")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--reload", dest="reload", action="store_true", help="Flag to force reloading data")
    group.add_argument("--no-reload", dest="reload", action="store_false", help="Flag to skip reloading data")
    parser.set_defaults(reload=True)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main(user_id=args.user_id, reload=args.reload))
