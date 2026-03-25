import asyncio
from typing import Optional, Iterator, Tuple

from embedded_db_mmap import MmapStorage


class AsyncStorage:
    def __init__(self, storage: MmapStorage):
        self.storage = storage
        self._lock = asyncio.Lock()

    # ---------- WRITE ----------

    async def put(self, key: bytes, value: bytes) -> None:
        async with self._lock:
            self.storage.put(key, value)

    # ---------- READ ----------

    async def get(self, key: bytes) -> Optional[bytes]:
        return self.storage.get(key)

    async def items(self) -> Iterator[Tuple[bytes, bytes]]:
        # snapshot semantics (best-effort)
        return list(self.storage.items())
