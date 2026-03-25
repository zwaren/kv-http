import os
import mmap
import struct
from typing import Dict, Optional, Iterator, Tuple

HEADER_STRUCT = struct.Struct("!II")  # key_len, val_len
HEADER_SIZE = HEADER_STRUCT.size

GROW_SIZE = 64 * 1024 * 1024  # 64MB


class MmapStorage:
    def __init__(self, path: str):
        self.path = path

        self.fd = os.open(path, os.O_RDWR | os.O_CREAT)
        self.file_size = os.lseek(self.fd, 0, os.SEEK_END)

        # preallocate, если файл пустой
        if self.file_size == 0:
            os.ftruncate(self.fd, GROW_SIZE)
            self.file_size = GROW_SIZE

        self.m = mmap.mmap(self.fd, self.file_size)

        self.index: Dict[bytes, int] = {}
        self.write_offset = 0

        self._load_index()

    # ---------- PUBLIC API ----------

    def put(self, key: bytes, value: bytes) -> None:
        record = self._encode_record(key, value)
        rec_len = len(record)

        # ensure capacity
        self._ensure_capacity(rec_len)

        offset = self.write_offset

        self.m[offset:offset + rec_len] = record

        self.index[key] = offset
        self.write_offset += rec_len

    def get(self, key: bytes) -> Optional[bytes]:
        offset = self.index.get(key)
        if offset is None:
            return None

        return self._read_value(offset)

    def items(self) -> Iterator[Tuple[bytes, bytes]]:
        for key, offset in self.index.items():
            yield key, self._read_value(offset)

    # ---------- INTERNAL ----------

    def _encode_record(self, key: bytes, value: bytes) -> bytes:
        return (
            HEADER_STRUCT.pack(len(key), len(value))
            + key
            + value
        )

    def _read_value(self, offset: int) -> bytes:
        header = self.m[offset:offset + HEADER_SIZE]
        key_len, val_len = HEADER_STRUCT.unpack(header)

        start = offset + HEADER_SIZE + key_len
        end = start + val_len

        return self.m[start:end]

    def _load_index(self) -> None:
        offset = 0

        while offset + HEADER_SIZE <= self.file_size:
            header = self.m[offset:offset + HEADER_SIZE]
            if len(header) < HEADER_SIZE:
                break

            key_len, val_len = HEADER_STRUCT.unpack(header)

            if key_len == 0 and val_len == 0:
                break  # дошли до пустого места (preallocation)

            key_start = offset + HEADER_SIZE
            key_end = key_start + key_len

            key = self.m[key_start:key_end]

            self.index[key] = offset

            offset += HEADER_SIZE + key_len + val_len

        self.write_offset = offset

    def _ensure_capacity(self, needed: int) -> None:
        if self.write_offset + needed <= self.file_size:
            return

        # увеличиваем файл
        new_size = self.file_size
        while self.write_offset + needed > new_size:
            new_size += GROW_SIZE

        self.m.close()
        os.ftruncate(self.fd, new_size)
        self.m = mmap.mmap(self.fd, new_size)

        self.file_size = new_size

    def close(self):
        self.m.flush()
        self.m.close()
        os.close(self.fd)
