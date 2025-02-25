import asyncio
import logging
import json

from utils import Endpoint


class ClientObserver:
    async def on_response(self, cluster: int, response: dict):
        raise NotImplementedError


class Client:
    _leaders: dict[int, int | None]    # cluster index -> leader index
    _clusters: dict[int, list[int]]    # cluster index -> list of member indices
    _server_eps: dict[int, Endpoint]          # member index -> endpoint
    _router_ep: Endpoint
    _observers: list[ClientObserver]
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    _logger: logging.Logger

    def __init__(self, clusters: dict[int, list[int]], server_eps: dict[int, Endpoint], router_ep: Endpoint, observers: list[ClientObserver], logger: logging.Logger = None):
        self._leaders = {cluster: None for cluster in clusters}
        self._clusters = clusters
        self._server_eps = server_eps
        self._router_ep = router_ep
        self._observers = observers
        self._reader = None
        self._writer = None
        self._logger = logger


    async def _connect_to_router(self):
        self._reader, self._writer = await asyncio.open_connection(self._router_ep.ip, self._router_ep.port)
        self._logger.info(f"Connected to router at {self._router_ep}")


    def get_cluster_from_ep(self, ep: Endpoint) -> int | None:
        for cluster, members in self._clusters.items():
            for member in members:
                if self._server_eps[member] == ep:
                    return cluster
        return None


    async def _read_from_router(self):
        # read from router and put into the queue
        # return when router closes connection
        try:
            while data := await self._reader.readuntil(b"\n"):
                self._logger.info(f"Received {data} from router")
                try:
                    data = json.loads(data.decode())
                except json.JSONDecodeError:
                    self._logger.error("Invalid message from router")
                    continue
                receive_from, content = data["from"], data["content"]
                cluster = self.get_cluster_from_ep(receive_from)
                if cluster is None:
                    self._logger.error(f"Unknown server {receive_from}")
                else:
                    for observer in self._observers:    # notify observers
                        await observer.on_response(cluster, content)
        except asyncio.IncompleteReadError:
            self._logger.info("Router closed connection")
        finally:
            self._writer.close()
            await self._writer.wait_closed()


    async def async_start(self):
        await self._connect_to_router()
        self_ip, self_port = self._writer.get_extra_info("sockname")
        self._logger.info("Client started at %s:%d", self_ip, self_port)
        await self._read_from_router()
        self._writer.close()
        await self._writer.wait_closed()
        self._logger.info("Client stopped")
        return