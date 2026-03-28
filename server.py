import asyncio
from typing import Dict, Optional, Tuple, List
from urllib.parse import urlparse
from asyncio import StreamReader, StreamWriter
import uuid

from db_lock_wrapper import AsyncStorage


STATUS_DESCS = {
    200: 'OK',
    404: 'Not Found',
    500: 'Internal Server Error',
}

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

    async def handle_ping(self, reader: StreamReader, writer: StreamWriter):
        try:
            while True:
                req = await reader.readuntil(b"\r\n\r\n")
                if req is None:
                    break

                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: keep-alive\r\n"
                    b"\r\n"
                    b"OK"
                )
                await writer.drain()

        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def handle_client(
        self,
        reader: StreamReader,
        writer: StreamWriter
    ) -> None:
        try:
            while True:
                request = await self._read_request(reader)
                if request is None:
                    return
                method, path, headers, body = request
                response = await self._route(method, path, body, writer)
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
        body: bytes,
        writer: StreamWriter,
    ):
        parsed = urlparse(path)
        parts: List[str] = parsed.path.strip("/").split("/")

        if method == "POST" and parts == ["records"]:
            await self._handle_post(body, writer)
            return

        if method == "GET" and parts[0] == "dump":
            await self._handle_dump(writer)
            return

        if method == "GET" and parts[0] == "records":
            if len(parts) == 2:
                await self._handle_get(parts[1], writer)
                return
            elif len(parts) == 1:
                await self._handle_get_all(writer)
                return

        writer.write(self._response(404, b"not found"))

    # ---------- HANDLERS ----------

    async def _handle_post(self, body: bytes, writer: StreamWriter):
        await self.storage.put(body[:16], body[16:])
        writer.write(self._response(200, b"OK"))

    async def _handle_get(self, key_str: str, writer: StreamWriter):
        key: bytes = uuid.UUID(key_str).bytes

        value: Optional[bytes] = await self.storage.get(key)
        if value is None:
            writer.write(self._response(404, b"not found"))
            return

        writer.write(self._response(200, value))

    async def _handle_get_all(self, writer: StreamWriter):
        items: List[Tuple[bytes, bytes]] = await self.storage.items()
        res = [item[0] + item[1] for item in items]
        writer.write(self._response(200, b'\n'.join(res)))
    
    async def _handle_dump(self, writer: StreamWriter):
        size = self.storage.storage.write_offset

        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"Content-Length: " + str(size).encode() + b"\r\n"
            b"Connection: keep-alive\r\n"
            b"\r\n"
        )

        CHUNK = 1 * 1024 * 1024
        view = memoryview(self.storage.storage.m)
        for i in range(0, size, CHUNK):
            await writer.drain()
            writer.write(view[i:min(i+CHUNK, size)])

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
            content_type="application/octet-stream",
            connection="keep-alive"
            ) -> bytes:
        return (
            f"HTTP/1.1 {status} {STATUS_DESCS[status]}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: {connection}\r\n"
            "\r\n"
        ).encode() + body