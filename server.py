import raft.server as raft
from utils import Endpoint


class Server(raft.Server):
    _lock_table: dict[int, Endpoint | None]    # lock index -> holder endpoint
    _balance_table: dict[int, int]    # account index -> balance

    def __init__(self, index: int, self_ep: Endpoint, router_ep: Endpoint, peer_eps: dict[int, Endpoint], logger = None, lock_table: dict[int, Endpoint | None] = None, balance_table: dict[int, int] = None):
        super().__init__(index, self_ep, router_ep, peer_eps, logger)
        self._lock_table = {} if lock_table is None else lock_table
        self._balance_table = {} if balance_table is None else balance_table


    def handle_lock_acquire


    def apply(self) -> list[dict]:
        results = []
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            command = self._storage[self._last_applied][1]
            content = command["content"]
            type = content["type"]
            if type == "LockAcquire":
                # TODO: put serial number in db