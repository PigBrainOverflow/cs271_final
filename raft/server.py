import asyncio
import logging
import json

from .policy import Policy, FollowerPolicy
from utils import Endpoint, PersistentStorage


class Server:
    # Network
    _index: int
    _self_ep: Endpoint
    _router_ep: Endpoint
    _peer_eps: dict[int, Endpoint]  # peer index -> endpoint, excluding itself
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    _queue: asyncio.Queue[dict]

    # Logging
    _logger: logging.Logger

    # Raft-related constants
    _ELECTION_TIMEOUT_MIN: int = 150    # milliseconds
    _ELECTION_TIMEOUT_MAX: int = 300
    _HEARTBEAT_INTERVAL: int = 50
    _MAX_ENTRIES_PER_APPEND_ENTRIES: int = 10

    # Raft-related peresistent states
    _storage: PersistentStorage

    # Raft-related volatile states
    _policy: Policy
    _commit_index: int
    _last_applied: int

    def apply(self) -> list[dict]:
        # apply log entries to state machine from last_applied + 1 to commit_index
        # update last_applied
        # return the list of results
        raise NotImplementedError


    def __init__(self, index: int, self_ep: Endpoint, router_ep: Endpoint, peer_eps: dict[int, Endpoint], logger = None):
        self._index = index
        self._self_ep = self_ep
        self._router_ep = router_ep
        self._peer_eps = peer_eps
        self._logger = logger
        self._reader = None
        self._writer = None
        self._queue = asyncio.Queue()
        self._storage = None
        self._policy = None
        self._commit_index = 0
        self._last_applied = 0


    async def _connect_to_router(self):
        self._reader, self._writer = await asyncio.open_connection(self._router_ep.ip, self._router_ep.port, local_addr=(self._self_ep.ip, self._self_ep.port))
        self._logger.info(f"Connected to router at {self._router_ep}")


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
                await self._queue.put(data) # put data into the queue
        except:
            self._logger.info("Router closed connection")
        finally:
            self._writer.close()


    async def _read_from_queue(self):
        # read from the queue and handle the message
        try:
            while True:
                msg = await self._queue.get()
                self._logger.info(f"Handling event {msg}")
                self._policy = await self._policy.handle_event(msg)
        except:
            pass


    async def async_start(self):
        self._storage = PersistentStorage(f"server{self._index}")
        # start as a follower
        self._policy = FollowerPolicy(self)
        try:
            await self._connect_to_router()
            _, pending = await asyncio.wait(
                [
                    asyncio.create_task(self._read_from_router()),
                    asyncio.create_task(self._read_from_queue())
                ],
                return_when=asyncio.FIRST_COMPLETED  # stop when any task completes
            )
            for task in pending:
                task.cancel()
        except:
            pass
        self._logger.info("Server terminated successfully")


    def start(self):
        asyncio.run(self.async_start())