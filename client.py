import logging
import asyncio

from utils import Endpoint
import raft.client as raft


class Client(raft.Client):
    # this client blocks until the response is received
    _loop: asyncio.AbstractEventLoop

    class UnexpectedResponseError(Exception):
        pass

    def __init__(self, clusters: dict[int, list[int]], server_eps: dict[int, Endpoint], router_ep: Endpoint, logger: logging.Logger = None, loop: asyncio.AbstractEventLoop = None):
        super().__init__(clusters, server_eps, router_ep, [], logger)
        self._serial_number = 0
        self._loop = asyncio.get_event_loop() if loop is None else loop


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
        # wait for the response
        response = await asyncio.wait_for(self.receive_response(), timeout=time)
        content = response["content"]
        if content["serial_number"] != self._serial_number:
            raise Client.UnexpectedResponseError()
        return content
        # raise asyncio.TimeoutError


    async def request_cluster(self, cluster: int, command: dict, time: float = 1.0, retries: int = 3) -> dict | None:
        # send the request to the leader of the cluster
        self._serial_number += 1
        for _ in range(retries):
            leader = self._leaders[cluster]
            leader = self._clusters[cluster][0] if leader is None else leader
            try:
                content = await self.request_server(leader, command, time)
                if content is None:
                    return None
                if "leader_id" in content.keys():
                    # if the leader has changed, update the leader
                    self._leaders[cluster] = content["leader_id"]
                else:
                    return content
            except asyncio.TimeoutError:
                # if the request times out, try again
                pass
        raise asyncio.TimeoutError


    def lock_acquire(self, cluster: int, item_id: int) -> dict | None:
        # raise asyncio.TimeoutError if the response is not received within the time limit
        # raise Client.UnexpectedResponseError if the serial number of the response is not the same as the request
        return self._loop.run_until_complete(self.request_cluster(cluster, {"type": "LockAcquire", "item_id": item_id}))