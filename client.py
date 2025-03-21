import logging
import asyncio

from utils import Endpoint
import raft.client as raft


class Client(raft.Client):
    # this client blocks until the response is received
    _loop: asyncio.AbstractEventLoop

    _item_to_cluster: dict[tuple[int, int], int]    # item_id range [left, right) -> cluster_id

    class UnexpectedResponseError(Exception):
        pass


    def __init__(self, clusters: dict[int, list[int]], server_eps: dict[int, Endpoint], router_ep: Endpoint, item_to_cluster: dict[tuple[int, int], int], logger: logging.Logger = None, loop: asyncio.AbstractEventLoop = None):
        super().__init__(clusters, server_eps, router_ep, [], logger)
        self._item_to_cluster = item_to_cluster
        self._serial_number = 0
        self._loop = asyncio.get_event_loop() if loop is None else loop


    def get_cluster_by_item(self, item_id: int) -> int | None:
        for (left, right), cluster in self._item_to_cluster.items():
            if left <= item_id and item_id < right:
                return cluster
        return None


    def start(self):
        self._loop.run_until_complete(self._connect_to_router())


    # def request_once(self, cluster: int, command: dict) -> dict | None:
    #     server_index = self._leaders[cluster]
    #     server_index = server_index if server_index is not None else self._clusters[cluster][0] # if no known leader, choose the first server
    #     self._loop.run_until_complete(self.send_request(server_index, command))
    #     # wait for the response
    #     while True:
    #         response = self._loop.run_until_complete(self.receive_response())
    #         content = response["content"]
    #         if content["serial_number"] == self._serial_number:
    #             self._serial_number += 1
    #             return content


    async def request_server(self, server: int, command: dict, time: float = 1.0) -> dict | None:
        # send the request to the server
        # block until the response is sent
        # raise asyncio.TimeoutError if the response is not received within the time limit
        # raise Client.UnexpectedResponseError if the serial number of the response is not the same as the request
        await self.send_request(server, command)
        self._logger.info(f"{command} sent to server {server}")
        # wait for the response
        try:
            response = await asyncio.wait_for(self.receive_response(), timeout=time)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError
        self._logger.info(f"{response} received from server {server}")
        content = response["content"]
        if content["serial_number"] != self._serial_number:
            raise Client.UnexpectedResponseError()
        return content
        # raise asyncio.TimeoutError


    async def request_cluster(self, cluster: int, command: dict, time: float = 1.0, retries: int = 3) -> dict | None:
        # send the request to the leader of the cluster
        self._serial_number += 1
        next_server_index = 0
        for _ in range(retries):
            leader = self._leaders[cluster]
            if leader is None:
                leader = self._clusters[cluster][next_server_index]
                next_server_index = (next_server_index + 1) % len(self._clusters[cluster])
            try:
                content = await self.request_server(leader, command, time)
                self._logger.info(content)
                # if content is None:
                #     # if the request times out, try again with another server
                #     self._leaders[cluster] = None
                if "leader_id" in content.keys():
                    # if the leader has changed, update the leader
                    self._leaders[cluster] = content["leader_id"]
                    self._logger.info(f"Leader is updated to {content['leader_id']}")
                else:
                    self._leaders[cluster] = leader
                    return content["response"]
            except asyncio.TimeoutError:
                # if the request times out, try again with another server
                self._leaders[cluster] = None
        return {"status": False, "reason": "Request failed after retries"}


    def _lock_acquire(self, cluster: int, item_id: int) -> dict | None:
        # raise asyncio.TimeoutError if the response is not received within the time limit
        # raise Client.UnexpectedResponseError if the serial number of the response is not the same as the request
        # raise asyncio.TimeoutError if the request times out
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "LockAcquire", "item_id": item_id}))


    def _lock_release(self, cluster: int, item_id: int) -> dict | None:
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "LockRelease", "item_id": item_id}))


    def _balance(self, cluster: int, item_id: int) -> dict | None:
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "Balance", "item_id": item_id}))


    def _deposit(self, cluster: int, item_id: int, amount: int) -> dict | None:
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "Deposit", "item_id": item_id, "amount": amount}))


    def _withdraw(self, cluster: int, item_id: int, amount: int) -> dict | None:
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "Withdraw", "item_id": item_id, "amount": amount}))


    def transfer(self, from_id: int, to_id: int, amount: int) -> dict | None:
        # two-phase commit & two-phase lock
        # to prevent deadlock, we always acquire locks in the order of item_id
        # raise Client.UnexpectedResponseError if the serial number of the response is not the same as the request
        # raise asyncio.TimeoutError if the request times out
        from_cluster = self.get_cluster_by_item(from_id)
        to_cluster = self.get_cluster_by_item(to_id)
        if from_cluster is None or to_cluster is None:
            return None
        # acquire locks
        if from_id < to_id:
            # acquire from_id first, then check from_id balance
            response_from_lock = self._lock_acquire(from_cluster, from_id)
            status, reason = response_from_lock["status"], response_from_lock["reason"]
            if not status:
                return {"status": False, "reason": reason}
            # we assume the balance is always successful
            response_from_balance = self._balance(from_cluster, from_id)
            balance = response_from_balance["value"]
            if balance < amount:
                self._lock_release(from_cluster, from_id)
                return {"status": False, "reason": "Insufficient balance"}
            # acquire to_id
            response_to_lock = self._lock_acquire(to_cluster, to_id)
            status, reason = response_to_lock["status"], response_to_lock["reason"]
            if not status:
                self._lock_release(from_cluster, from_id)
                return {"status": False, "reason": reason}
        else:   # from_id > to_id
            # acquire to_id first, then acquire from_id
            response_to_lock = self._lock_acquire(to_cluster, to_id)
            status, reason = response_to_lock["status"], response_to_lock["reason"]
            if not status:
                return {"status": False, "reason": reason}
            response_from_lock = self._lock_acquire(from_cluster, from_id)
            status, reason = response_from_lock["status"], response_from_lock["reason"]
            if not status:
                self._lock_release(to_cluster, to_id)
                return {"status": False, "reason": reason}
            response_from_balance = self._balance(from_cluster, from_id)
            balance = response_from_balance["value"]
            if balance < amount:
                self._lock_release(to_cluster, to_id)
                self._lock_release(from_cluster, from_id)
                return {"status": False, "reason": "Insufficient balance"}
        # deposit to to_id & withdraw from from_id
        # we assume the deposit and withdraw operations are always successful
        self._deposit(to_cluster, to_id, amount)
        self._withdraw(from_cluster, from_id, amount)
        # release locks
        # we assume the lock release operations are always successful
        self._lock_release(from_cluster, from_id)
        self._lock_release(to_cluster, to_id)
        return {"status": True, "reason": None}


    def balance(self, item_id: int) -> dict | None:
        cluster = self.get_cluster_by_item(item_id)
        if cluster is None:
            return None
        # acquire lock
        response_lock = self._lock_acquire(cluster, item_id)
        status, reason = response_lock["status"], response_lock["reason"]
        if not status:
            return {"status": False, "reason": reason}
        # get balance
        response_balance = self._balance(cluster, item_id)
        # release lock
        self._lock_release(cluster, item_id)
        return response_balance


    def deposit(self, item_id: int, amount: int) -> dict | None:
        cluster = self.get_cluster_by_item(item_id)
        if cluster is None:
            return None
        # acquire lock
        response_lock = self._lock_acquire(cluster, item_id)
        status = response_lock["status"]
        if not status:
            return response_lock
        # deposit
        response_deposit = self._deposit(cluster, item_id, amount)
        # release lock
        self._lock_release(cluster, item_id)
        return response_deposit


    def withdraw(self, item_id: int, amount: int) -> dict | None:
        cluster = self.get_cluster_by_item(item_id)
        if cluster is None:
            return None
        # acquire lock
        response_lock = self._lock_acquire(cluster, item_id)
        status = response_lock["status"]
        if not status:
            return response_lock
        # check balance
        response_balance = self._balance(cluster, item_id)
        balance = response_balance["value"]
        if balance < amount:
            self._lock_release(cluster, item_id)
            return {"status": False, "reason": "Insufficient balance"}
        # withdraw
        response_withdraw = self._withdraw(cluster, item_id, amount)
        # release lock
        self._lock_release(cluster, item_id)
        return response_withdraw
