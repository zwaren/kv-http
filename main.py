import asyncio

from db_lock_wrapper import AsyncStorage
from embedded_db_mmap import MmapStorage
from server import HTTPServer


if __name__ == "__main__":
    storage = AsyncStorage(MmapStorage("data.db"))
    server = HTTPServer(storage)

    asyncio.run(server.start())