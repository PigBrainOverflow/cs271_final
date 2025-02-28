import raft.server as raft
from utils import Endpoint


class Server(raft.Server):
    _lock_table: dict[int, Endpoint | None]    # lock index -> holder endpoint
    _balance_table: dict[int, int]    # account index -> balance

    def __init__(self, index: int, self_ep: Endpoint, router_ep: Endpoint, peer_eps: dict[int, Endpoint], logger = None, lock_table: dict[int, Endpoint | None] = None, balance_table: dict[int, int] = None):
        super().__init__(index, self_ep, router_ep, peer_eps, logger)
        self._lock_table = {} if lock_table is None else lock_table
        self._balance_table = {} if balance_table is None else balance_table


    def _handle_lock_acquire(self, entry: tuple[int, str, int, int, dict, dict | None]) -> dict:
        _, ip, port, serial_number, command, _ = entry
        item_id = command["item_id"]
        status, reason = False, None
        if item_id in self._lock_table:
            if self._lock_table[item_id] is None:
                # lock is available
                self._lock_table[item_id] = Endpoint(ip, port)
                status = True
            elif self._lock_table[item_id] == Endpoint(ip, port):
                # already acquired
                # non-reentrant lock
                reason = "Already acquired"
            else:
                # lock is held by someone else
                reason = "Occupied"
        else:
            # item_id not found
            reason = "Item not found"
        return {
            "ip": ip,
            "port": port,
            "serial_number": serial_number,
            "response": {
                "status": status,
                "reason": reason
            }
        }


    def _handle_lock_release(self, entry: tuple[int, str, int, int, dict, dict | None]) -> dict:
        _, ip, port, serial_number, command, _ = entry
        item_id = command["item_id"]
        status, reason = False, None
        if item_id in self._lock_table:
            if self._lock_table[item_id] == Endpoint(ip, port):
                # lock is held by the requester
                self._lock_table[item_id] = None
                status = True
            elif self._lock_table[item_id] is None:
                # lock is available
                reason = "Already released"
            else:
                # lock is held by someone else
                reason = "Occupied"
        else:
            # item_id not found
            reason = "Item not found"
        return {
            "ip": ip,
            "port": port,
            "serial_number": serial_number,
            "response": {
                "status": status,
                "reason": reason
            }
        }


    def _handle_balance(self, entry: tuple[int, str, int, int, dict, dict | None]) -> dict:
        _, ip, port, serial_number, command, _ = entry
        item_id = command["item_id"]
        status, value = False, None
        if item_id in self._balance_table:
            status = True
            value = self._balance_table[item_id]
        return {
            "ip": ip,
            "port": port,
            "serial_number": serial_number,
            "response": {
                "status": status,
                "value": value
            }
        }


    def _handle_deposit(self, entry: tuple[int, str, int, int, dict, dict | None]) -> dict:
        _, ip, port, serial_number, command, _ = entry
        item_id, amount = command["item_id"], command["amount"]
        status, = False
        if item_id in self._balance_table:
            status = True
            self._balance_table[item_id] += amount
        return {
            "ip": ip,
            "port": port,
            "serial_number": serial_number,
            "response": {
                "status": status
            }
        }


    def _handle_withdraw(self, entry: tuple[int, str, int, int, dict, dict | None]) -> dict:
        # NOTE: this withdraw is a primitive assuming sufficient balance
        # you need to check the balance before calling this function
        _, ip, port, serial_number, command, _ = entry
        item_id, amount = command["item_id"], command["amount"]
        status  = False
        if item_id in self._balance_table:
            status = True
            self._balance_table[item_id] -= amount
        return {
            "ip": ip,
            "port": port,
            "serial_number": serial_number,
            "response": {
                "status": status
            }
        }


    def apply(self) -> list[dict]:
        results = []
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            entry = self._storage[self._last_applied]
            type = entry[4]["type"]
            self._logger.info(f"Applying {type} at index {self._last_applied}")
            if type == "LockAcquire":
                result = self._handle_lock_acquire(entry)
            elif type == "LockRelease":
                result = self._handle_lock_release(entry)
            elif type == "Balance":
                result = self._handle_balance(entry)
            elif type == "Deposit":
                result = self._handle_deposit(entry)
            elif type == "Withdraw":
                result = self._handle_withdraw(entry)
            else:
                raise ValueError(f"Unknown command type: {type}")
            # update log result
            self._storage.set_result(self._last_applied, result["response"])
            results.append(result)
        return results