import logging
import asyncio

from utils import Endpoint
import raft.client as raft


class Client(raft.Client):
    # this client blocks until the response is received
    _loop: asyncio.AbstractEventLoop

    def __init__(self, clusters: dict[int, list[int]], server_eps: dict[int, Endpoint], router_ep: Endpoint, logger: logging.Logger = None, loop: asyncio.AbstractEventLoop = None):
        super().__init__(clusters, server_eps, router_ep, [], logger)
        self._serial_number = 0
        self._loop = asyncio.get_event_loop() if loop is None else loop


    def start(self):
        asyncio.run_coroutine_threadsafe(self._connect_to_router(), self._loop).result()


    def request(self, cluster: int, command: dict) -> dict | None:
        server_index = self._leaders[cluster]
        server_index = server_index if server_index is not None else self._clusters[cluster][0]
        asyncio.run_coroutine_threadsafe(self.send_request(server_index, self._serial_number, command), self._loop).result()
        # wait for the response
        while True:
            response = asyncio.run_coroutine_threadsafe(self.receive_response(), self._loop).result()
            if response["serial_number"] == self._serial_number:
                return response["response"]