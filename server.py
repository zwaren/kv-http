import asyncio
from typing import Dict, Optional, Tuple, List
from urllib.parse import urlparse
from asyncio import StreamReader, StreamWriter
import uuid

from db_lock_wrapper import AsyncStorage


Request = Tuple[str, str, Dict[str, str], bytes]


class HTTPServer:
    def __init__(
        self,
        storage: AsyncStorage,
        host: str = "127.0.0.1",
        port: int = 8080
    ) -> None:
        self.storage = storage
        self.host = host
        self.port = port

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        async with server:
            print(f"Serving on {self.host}:{self.port}")
            await server.serve_forever()

    # ---------- CORE ----------

    async def handle_client(
        self,
        reader: StreamReader,
        writer: StreamWriter
    ) -> None:
        try:
            request = await self._read_request(reader)
            if request is None:
                return

            method, path, headers, body = request

            response = await self._route(method, path, body)
            writer.write(response)

            await writer.drain()
        except Exception as e:
            writer.write(self._response(500, str(e).encode()))
        finally:
            writer.close()
            await writer.wait_closed()

    # ---------- ROUTER ----------

    async def _route(
        self,
        method: str,
        path: str,
        body: bytes
    ) -> bytes:
        parsed = urlparse(path)
        parts: List[str] = parsed.path.strip("/").split("/")

        if method == "POST" and parts == ["records"]:
            return await self._handle_post(body)

        if method == "GET" and parts[0] == "dump":
            return await self._handle_dump()

        if method == "GET" and parts[0] == "records":
            if len(parts) == 2:
                return await self._handle_get(parts[1])
            elif len(parts) == 1:
                return await self._handle_get_all()

        return self._response(404, b"not found")

    # ---------- HANDLERS ----------

    async def _handle_post(self, body: bytes) -> bytes:
        await self.storage.put(body[:16], body[16:])
        return self._response(200, b"OK")

    async def _handle_get(self, key_str: str) -> bytes:
        key: bytes = uuid.UUID(key_str).bytes

        value: Optional[bytes] = await self.storage.get(key)
        if value is None:
            return self._response(404, b"not found")

        return self._response(200, value)

    async def _handle_get_all(self) -> bytes:
        items: List[Tuple[bytes, bytes]] = await self.storage.items()
        res = [item[0] + item[1] for item in items]
        return self._response(200, b'\n'.join(res))
    
    async def _handle_dump(self) -> bytes:
        data = self.storage.storage.m[:self.storage.storage.write_offset]
        return self._response(200, data, "application/octet-stream")

    # ---------- HTTP PARSING ----------

    async def _read_request(
        self,
        reader: StreamReader
    ) -> Optional[Request]:
        headers: bytes = b""

        while True:
            line: bytes = await reader.readline()
            if not line:
                return None

            headers += line
            if headers.endswith(b"\r\n\r\n"):
                break

        header_text: str = headers.decode()
        lines: List[str] = header_text.split("\r\n")

        request_line: str = lines[0]
        method, path, _ = request_line.split()

        headers_dict: Dict[str, str] = {}
        for line in lines[1:]:
            if line:
                k, v = line.split(":", 1)
                headers_dict[k.strip().lower()] = v.strip()

        content_length: int = int(headers_dict.get("content-length", 0))

        body: bytes = b""
        if content_length > 0:
            body = await reader.readexactly(content_length)

        return method, path, headers_dict, body

    # ---------- RESPONSE ----------

    def _response(
            self,
            status: int,
            body: bytes,
            content_type="application/octet-stream"
            ) -> bytes:
        return (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode() + body