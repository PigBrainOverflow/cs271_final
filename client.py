import fastapi
import json
import uvicorn
import time
import os
import logging
from utils import get_current_time, Account, Transaction, Endpoint
import numpy as np
from pprint import pprint
from typing import List
import argparse
import httpx  # Use httpx for async requests

with open('config.json') as f:
    CONFIG = json.load(f)

partition = {1: (1, 1001), 2: (1001, 2001), 3: (2001, 3001)}

class BankClient:
    """Bank server"""
    __instance = None  # Singleton pattern

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self) -> None:
        self.accounts: List[Account] = []  # Account information for clients
        self.clients = {}  # Client addresses: {id1: addr1, id2: addr2, ...}
        self.ipv4 = CONFIG['HOST_IPv4']
        self.port = CONFIG['HOST_PORT']

        self.router = fastapi.APIRouter()
        self.router.add_api_route('/Hbalance/{client_id}', self.Hbalance, methods=['POST'])
        self.router.add_api_route('/Htxnlog', self.Htxnlog, methods=['POST'])
        self.router.add_api_route('/Htransfer', self.Htransfer, methods=['POST'])

    # for fastapi route part
    async def Hbalance(self, client_id: int): # handle balance request from user
        for server_id, (start_id, end_id) in partition.items():
            if start_id <= client_id <= (end_id - 1):
                async with httpx.AsyncClient() as client:
                    server_address = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], 8000 + server_id)
                    await client.post(f"{server_address}/balance/{client_id}")

    async def Htxnlog(self):
        for server_id, (start_id, end_id) in partition.items():
            async with httpx.AsyncClient() as client:
                server_address = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], 8000 + server_id)
                res = await client.post(f"{server_address}/txnlog")

    async def Htransfer(self, trans: Transaction):
        print(trans.model_dump())
        x_serveraddr = None
        y_serveraddr = None
        for server_id, (start_id, end_id) in partition.items():
            if start_id <= trans.x <= (end_id - 1):
                x_serveraddr = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], 8000 + server_id)
            if start_id <= trans.y <= (end_id - 1):
                y_serveraddr = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], 8000 + server_id)

        if not x_serveraddr or not y_serveraddr:
            return {"error": "No matching server found for x or y"}

        async with httpx.AsyncClient() as client:
            if x_serveraddr == y_serveraddr: # intra-shard if x,y same server
                shard = 1
                res = await client.post(f"{x_serveraddr}/transfer/{shard}", json=trans.model_dump())
                data = res.json()
                if not data['status']:
                    # print(f"{data['reason']}")
                    return {'reason': data['reason']}
            else: # cross-shard if x,y same server
                shard = 0
                xpacket = Endpoint(id=trans.x, ip=self.ipv4, port=self.port)
                ypacket = Endpoint(id=trans.y, ip=self.ipv4, port=self.port)
                res = await client.post(f"{x_serveraddr}/lockacq", json=xpacket.model_dump())
                data = res.json()
                if not data['status']:
                    # print(f"{data['reason']}")
                    return {'reason': data['reason']}
                res = await client.post(f"{y_serveraddr}/lockacq", json=ypacket.model_dump())
                data = res.json()
                if not data['status']:
                    # print(f"{data['reason']}")
                    return {'reason': data['reason']}
                res = await client.post(f"{x_serveraddr}/transfer/{shard}", json=trans.model_dump())
                data = res.json()
                if not data['status']:
                    await client.post(f"{x_serveraddr}/lockrel", json=xpacket.model_dump())
                    await client.post(f"{y_serveraddr}/lockrel", json=ypacket.model_dump())
                    # print(f"{data['reason']}")
                    return {'reason': data['reason']}
                res = await client.post(f"{y_serveraddr}/transfer/{shard}", json=trans.model_dump())
                data = res.json()
                if not data['status']:
                    res = await client.post(f"{x_serveraddr}/lockrel", json=xpacket.model_dump())
                    res = await client.post(f"{y_serveraddr}/lockrel", json=ypacket.model_dump())
                    # print(f"{data['reason']}")
                    return {'reason': data['reason']}
                res = await client.post(f"{x_serveraddr}/lockrel", json=xpacket.model_dump())
                res = await client.post(f"{y_serveraddr}/lockrel", json=ypacket.model_dump())

        return {'status': True}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Python client example with port argument.")
    parser.add_argument("-p", "--port", type=int, required=True, help="Port number to connect to")
    args = parser.parse_args()

    server = BankClient()
    server.port = args.port + CONFIG['HOST_PORT']
    print(f"server port {server.port}")

    app = fastapi.FastAPI()
    app.include_router(server.router)

    uvicorn.run(
        app,
        host=server.ipv4,
        port=server.port,
        log_level='warning'
    )