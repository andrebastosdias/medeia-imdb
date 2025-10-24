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

import utils
import imdb


PandasLike = TypeVar('PandasLike', pd.DataFrame, pd.Series)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATASET_ROOT = "medeia"
DATASET_BASE = DATASET_ROOT
DATASET_FILMS = DATASET_ROOT + "-films"

MEDEIA_URL = "https://medeiafilmes.com/filmes-em-exibicao"
DATA_PATTERN = re.compile(r"global\.data\s*=\s*(\{.*?\});")
TARGET_THEATERS = ["cinema-medeia-nimas"]

async def handle(ctx: BeautifulSoupCrawlingContext) -> None:
    html = await imdb.get_response(ctx.http_response)
    hit = DATA_PATTERN.search(html)
    if not hit:
        ctx.log.error("No global.data on %s", ctx.request.url)
        return

    data = json.loads(hit.group(1))
    await ctx.push_data({
        "url": ctx.request.url,
        "data": data,
    }, dataset_alias=DATASET_BASE if ctx.request.crawl_depth == 0 else DATASET_FILMS)

    if ctx.request.crawl_depth == 0:
        movies = data["schedule"]["events"]
        await ctx.enqueue_links(requests=[
            movie_info['url'] for movie_id, movie_info in movies.items() if movie_id.startswith('film-')
        ])

def extract_film_data(data: dict):
    film_data = data["data"]["film"]

    def extract_sessions() -> list[datetime]:
        sessions: list[datetime] = []
        for theater in film_data["programme"].values():
            if theater.get("slug") in TARGET_THEATERS:
                for session in theater["sessions"].values():
                    date = session["date"]
                    hours = session["hours"]
                    hours = [str(hour).replace("*", "").strip() for hour in hours]
                    sessions.extend(
                        utils.to_datetime(f"{date} {hour}", "%Y-%m-%d %H:%M")
                        for hour in hours if hour
                    )
        return sorted(sessions)

    return {
        "id": int(film_data["id"]),
        "original_title": film_data["title_original"].strip(),
        "title": film_data["title"].strip(),
        "director": film_data["director_name"].strip(),
        "cast": [cast.strip() for cast in film_data["cast"].split(",")],
        "release_year": int(film_data["production_year"]),
        "runtime": utils.string_to_runtime(film_data["length"]) if film_data.get("length") else pd.NA,
        "sessions": extract_sessions(),
        "url": data["url"],
    }

async def get_medeia_movies() -> pd.DataFrame:
    crawler = BeautifulSoupCrawler(http_client=imdb.HTTP_CLIENT, max_crawl_depth=1)
    crawler.router.default_handler(handle)
    crawler.failed_request_handler(imdb.on_failed_handler)
    await crawler.run([MEDEIA_URL])

    ds = await Dataset.open(alias=DATASET_FILMS)
    content = await ds.get_data()
    rows = [extract_film_data(item) for item in content.items]
    df_medeia = pd.DataFrame.from_records(rows, index='id')

    return df_medeia

def match_series_imdb(movie_row: pd.Series, df_imdb: pd.DataFrame):
    movie_id = movie_row.name
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
    df_matches['directors'] = df_imdb['directors'].apply(lambda directors: any(director in movie_row['director'] for director in directors))
    df_matches['cast'] = df_imdb['cast'].apply(lambda cast: bool(set(movie_row['cast']).intersection(cast)))
    df_matches['release_year'] = (df_imdb['release_year'] - movie_row['release_year']).abs() <= 1
    df_matches['runtime'] = (
        df_imdb['runtime'].notna() & (df_imdb['runtime'] == movie_row['runtime'] * 60)
    )

    df_matches = df_matches[df_matches['title'] & df_matches['directors'] & df_matches['release_year']]
    if df_matches.shape[0] > 1:
        df_title_matches = df_matches[df_matches['title']]
        if df_title_matches.shape[0] == 1:
            df_matches = df_title_matches
    assert df_matches.shape[0] <= 1, f"Multiple matches found for movie: {movie_row['title']}:\n{df_matches}"

    if df_matches.shape[0] == 1:
        match = cast(pd.Series, original_df_imdb.loc[df_matches.index[0]])
        imdb_id = match.name
        watched = match['watched']
        imdb_lists = match['imdb_lists']
    else:
        imdb_id = pd.NA
        watched = False
        imdb_lists = []

    return pd.Series({
        'imdb_id': imdb_id,
        'watched': watched,
        'imdb_lists': imdb_lists,
    }, name=movie_id)

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
            df_imdb_watchlist['imdb_lists_dup'],
            lambda x, y: (x if isinstance(x, list) else []) + (y if isinstance(y, list) else [])
        )
        df_imdb_watchlist.drop('imdb_lists_dup', axis=1, inplace=True)

    df_imdb_watchlist = df_imdb_watchlist.convert_dtypes()
    df_imdb_watchlist['watched'] = df_imdb_watchlist['watched'].fillna(False)
    df_imdb_watchlist = df_imdb_watchlist.reindex(columns, axis=1)
    return df_imdb_watchlist

def get_sessions(df_movies: pd.DataFrame) -> pd.DataFrame:
    df_sessions = df_movies.dropna(subset=['sessions']).explode('sessions')
    df_sessions.rename(columns={'sessions': 'session'}, inplace=True)
    df_sessions.dropna(subset=['session'], inplace=True)

    df_sessions = df_sessions[df_sessions['session'] >= utils.midnight(utils.now())]

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

    df_sessions.sort_values(by=['session', 'id'], inplace=True)
    df_sessions.reset_index(inplace=True)
    return df_sessions

async def main_medeia():
    file_path = DATA_DIR / "movies.csv"

    df_movies = await get_medeia_movies()
    if os.path.exists(file_path):
        df_movies_prev = pd.read_csv(
            file_path,
            index_col='id',
            converters={
                "cast": ast.literal_eval,
                "runtime": utils.string_to_runtime,
                "imdb_lists": ast.literal_eval,
            },
            encoding='utf-8-sig'
        )
        df_movies = df_movies.combine_first(df_movies_prev)[df_movies.columns]
    df_movies.sort_index(inplace=True)

    df_sessions = get_sessions(df_movies)

    df_movies['runtime'] = df_movies['runtime'].apply(
        lambda x: utils.runtime_to_string(x) if pd.notna(x) else pd.NA
    )
    df_movies['sessions'] = df_movies['sessions'].apply(
        lambda x: [utils.to_string(s) for s in x] if isinstance(x, list) else pd.NA
    )
    df_movies.drop(columns=['sessions'], inplace=True)
    df_movies.to_csv(DATA_DIR / "movies.csv", index=True, encoding='utf-8-sig')

    df_sessions['session'] = df_sessions['session'].apply(utils.to_string)
    df_sessions = df_sessions[['id', 'session']]
    df_sessions.to_csv(DATA_DIR / "sessions.csv", index=False, encoding='utf-8-sig')

    return df_movies, df_sessions

async def main_imdb(user_id: str, df_movies: pd.DataFrame):
    df_imdb = await get_imdb_movies(user_id)
    df_imdb = df_movies.apply(
        match_series_imdb, axis=1, args=(df_imdb,), result_type='expand'
    )
    df_imdb = df_imdb.dropna(subset=['imdb_id'])
    df_imdb.to_csv(DATA_DIR / "imdb.csv", index=True, encoding='utf-8-sig')

    return df_imdb

async def main(user_id: str | None, reload_medeia: bool, reload_imdb: bool):
    df_movies = None
    df_sessions = None
    df_imdb = None
    if reload_medeia:
        df_movies, df_sessions = await main_medeia()
    if reload_imdb:
        assert user_id is not None
        if df_movies is None:
            file_path = DATA_DIR / "movies.csv"
            assert os.path.exists(file_path)
            df_movies = pd.read_csv(
                file_path,
                index_col='id',
                converters={
                    "cast": ast.literal_eval,
                    "runtime": utils.string_to_runtime,
                    "imdb_lists": ast.literal_eval,
                },
                encoding='utf-8-sig'
            )
        df_imdb = await main_imdb(user_id, df_movies)
    return df_movies, df_sessions, df_imdb

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the main async process.")
    parser.add_argument("--user-id", "-u", type=str, help="The user_id to process")
    parser.add_argument("--no-medeia", dest='reload_medeia', action='store_false', help="Disable Medeia data fetching")
    parser.add_argument("--no-imdb", dest='reload_imdb', action='store_false', help="Disable IMDB data fetching")
    parser.set_defaults(reload_medeia=True, reload_imdb=True)
    args = parser.parse_args()

    if args.reload_imdb and not args.user_id:
        parser.error("--user-id is required unless --no-imdb is specified")

    if not args.reload_medeia and not args.reload_imdb:
        parser.error("At least one of --medeia or --imdb must be enabled")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(main(user_id=args.user_id, reload_medeia=args.reload_medeia, reload_imdb=args.reload_imdb))
