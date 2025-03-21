import socket
import json

from utils import Endpoint


class User:
    _self_ep: Endpoint
    _router_ep: Endpoint
    _server_eps: dict[int, Endpoint]
    _conn: socket.socket

    def __init__(self, self_ep: Endpoint, router_ep: Endpoint, server_eps: dict[int, Endpoint]):
        self._self_ep = self_ep
        self._router_ep = router_ep
        self._server_eps = server_eps
        self._conn = None


    def connect_to_router(self):
        self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._conn.bind((self._self_ep.ip, self._self_ep.port))
        self._conn.connect((self._router_ep.ip, self._router_ep.port))


    def crash(self, targets: list[int]):
        command = {
            "command": "crash",
            "targets": [
                {
                    "ip": self._server_eps[target].ip,
                    "port": self._server_eps[target].port
                }
                for target in targets
            ]
        }
        self._conn.send(json.dumps(command).encode() + b"\n")


    def recover(self, targets: list[int]):
        command = {
            "command": "recover",
            "targets": [
                {
                    "ip": self._server_eps[target].ip,
                    "port": self._server_eps[target].port
                }
                for target in targets
            ]
        }
        self._conn.send(json.dumps(command).encode() + b"\n")


    def terminate(self):
        self._conn.send(json.dumps({"command": "terminate"}).encode() + b"\n")


    def exit(self):
        self._conn.close()
