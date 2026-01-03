import argparse
import asyncio
import json
import logging
import re
import unicodedata
from typing import cast
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from crawlee import HttpHeaders
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext, BasicCrawlingContext
from crawlee.http_clients import CurlImpersonateHttpClient, HttpResponse
from crawlee.storages import Dataset


logger = logging.getLogger(__name__)

WATCHLIST_URL = f"https://www.imdb.com/user/{{user_id}}/watchlist/?page={{page}}"
LISTS_URL = f"https://www.imdb.com/user/{{user_id}}/lists/"
LIST_URL = f"https://www.imdb.com/list/{{list_id}}/?page={{page}}"
DATA_PATTERN = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(\{.*?\})</script>')
USER_ID_PATTERN = re.compile(r"^https://www.imdb.com/user/([^/]+)/")
LIST_ID_PATTERN = re.compile(r"^https://www.imdb.com/list/([^/]+)/")

DATASET_ROOT = "imdb"
DATABASE_WATCHLIST = DATASET_ROOT + "-watchlist"
DATABASE_LISTS = DATASET_ROOT + "-lists"
DATASET_LIST = DATABASE_LISTS + f"-{{name}}"

HTTP_CLIENT = CurlImpersonateHttpClient(
    impersonate="chrome124",
    headers=HttpHeaders({
        "accept-encoding": "gzip, deflate, br",
    }),
)

def to_ascii(text: str, sep: str = "") -> str:
    ascii_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    ascii_text = re.sub(r'[^a-zA-Z0-9{}]'.format(re.escape(sep)), sep, ascii_text)
    if sep:
        ascii_text = re.sub(r'{}+'.format(re.escape(sep)), sep, ascii_text)
    ascii_text = ascii_text.strip(sep or None).lower()
    return ascii_text

async def get_response(http_response: HttpResponse) -> str:
    content_type = http_response.headers.get("content-type", "")
    encoding = (
        content_type.split("charset=")[1]
        if "charset=" in content_type else "utf-8"
    )
    content = await http_response.read()
    return content.decode(encoding, errors="replace")

def build_next_url(current_url: str) -> str:
    parts = urlparse(current_url)
    query = parse_qs(parts.query)
    next_page = int(query["page"][0]) + 1
    query["page"] = [str(next_page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parts._replace(query=new_query))

async def handle_movies(ctx: BeautifulSoupCrawlingContext) -> None:
    url = ctx.request.url

    html = await get_response(ctx.http_response)
    hit = DATA_PATTERN.search(html)
    if not hit:
        raise RuntimeError(f"No __NEXT_DATA__ on {url}")

    data = json.loads(hit.group(1))
    main_column_data = data["props"]["pageProps"]["mainColumnData"]

    request_type = "watchlist" if "watchlist" in url else "lists" if "lists" in url else "list"
    list_name = main_column_data["list"]["name"]["originalText"] if request_type == "list" else None

    await ctx.push_data({
        "url": url,
        "data": main_column_data,
    }, dataset_alias=(
        DATABASE_LISTS if request_type == "lists" else
        DATABASE_WATCHLIST if request_type == "watchlist" else
        DATASET_LIST.format(name=to_ascii(cast(str, list_name)))
    ))

    if request_type == "lists":
        user_list_search = main_column_data["userListSearch"]
        await ctx.enqueue_links(requests=[
            LIST_URL.format(list_id=edge["node"]["id"], page=1)
            for edge in user_list_search["edges"]
        ])
    else:
        title_list_item_search = main_column_data["predefinedList" if request_type == "watchlist" else "list"]["titleListItemSearch"]
        if title_list_item_search["pageInfo"]["hasNextPage"]:
            await ctx.enqueue_links(requests=[build_next_url(url)])

def on_failed_handler(ctx: BasicCrawlingContext, err: Exception):
    logger.error(f"Request {ctx.request.url} failed after retries: {err}")
    raise RuntimeError(f"Request {ctx.request.url} failed after retries: {err}")

def extract_movie_data(data: dict) -> dict:
    movie_data = data["listItem"]

    directors = []
    cast = []
    if "principalCredits" in movie_data:
        for principal in movie_data["principalCredits"]:
            if principal["category"]["id"] == "director":
                directors += [director["name"]["nameText"]["text"].strip() for director in principal["credits"]]
            elif principal["category"]["id"] == "cast":
                cast += [actor["name"]["nameText"]["text"].strip() for actor in principal["credits"]]
    elif "principalCreditsV2" in movie_data:
        for principal in movie_data["principalCreditsV2"]:
            if principal["grouping"]["text"] == "Director" or principal["grouping"]["text"] == "Directors":
                directors += [director["name"]["nameText"]["text"].strip() for director in principal["credits"]]
            elif principal["grouping"]["text"] == "Star" or principal["grouping"]["text"] == "Stars":
                cast += [actor["name"]["nameText"]["text"].strip() for actor in principal["credits"]]
    else:
        raise ValueError("No principalCredits or principalCreditsV2 in movie data")

    return {
        "id": movie_data["id"].strip(),
        "original_title": movie_data["originalTitleText"]["text"].strip(),
        "title": movie_data["titleText"]["text"].strip(),
        "directors": directors,
        "cast": cast,
        "release_year": movie_data["releaseYear"]["year"],
        "runtime": movie_data["runtime"]["seconds"] if movie_data.get("runtime") else None,
    }

async def get_lists(user_id: str) -> tuple[list[dict], dict[str, list[dict]]]:
    crawler = BeautifulSoupCrawler(http_client=HTTP_CLIENT)
    crawler.router.default_handler(handle_movies)
    crawler.failed_request_handler(on_failed_handler)
    await crawler.run([WATCHLIST_URL.format(user_id=user_id, page=1), LISTS_URL.format(user_id=user_id)])

    ds = await Dataset.open(alias=DATABASE_WATCHLIST)
    content = await ds.get_data()
    watchlist = [
        extract_movie_data(edge)
        for item in content.items
        for edge in item["data"]["predefinedList"]["titleListItemSearch"]["edges"]
    ]

    ds = await Dataset.open(alias=DATABASE_LISTS)
    content = await ds.get_data()
    lists: dict[str, list[dict]] = {}
    for edge in content.items[0]["data"]["userListSearch"]["edges"]:
        list_name = edge["node"]["name"]["originalText"]
        list_ds = await Dataset.open(alias=DATASET_LIST.format(name=to_ascii(list_name)))
        list_content = await list_ds.get_data()
        lists[list_name] = [
            extract_movie_data(edge)
            for item in list_content.items
            for edge in item["data"]["list"]["titleListItemSearch"]["edges"]
        ]
    return watchlist, lists

async def main(user_id: str) -> tuple[list[dict], dict[str, list[dict]]]:
    return await get_lists(user_id=user_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", "-u", type=str, required=True, help="The user_id to process")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main(user_id=args.user_id))
