import asyncio
import aiohttp
import json
from aiohttp_retry import RetryClient, ExponentialRetry
import time

from pokemon.mongo_client import MongoClient

POKEMON_DATA = []
CONFIG_FILE = "config.json"


async def fetch_url(client, url):
    async with client.get(url, ssl=False) as response:
        if response.status == 200:
            data = await response.json()
            POKEMON_DATA.append(data)
        else:
            return f"Failed with status: {response.status} {url}"


def get_json_details():
    with open(CONFIG_FILE, "r") as infile:
        data = json.load(infile)

    pokemon_api_details = data.get("pokemon_api_details", {})
    return pokemon_api_details.get("base_url"), pokemon_api_details.get("count", 0)


async def fetch_with_retry():
    url, count = get_json_details()
    retry_options = ExponentialRetry(attempts=5)
    async with RetryClient(raise_for_status=False, retry_options=retry_options) as client:
        tasks = []
        for number in range(1, count):
            tasks.append(fetch_url(client=client, url=url + str(number)))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    start_time = time.perf_counter()
    asyncio.run(fetch_with_retry())
    pymongo_client = MongoClient(config_file=CONFIG_FILE)
    pymongo_client.insert_many(data=POKEMON_DATA)
    print(time.perf_counter() - start_time)
