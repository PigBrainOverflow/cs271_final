import fastapi
import json
import uvicorn
import time
import os
import threading
import logging
from utils import get_current_time, Account
import numpy as np
from pprint import pprint
from typing import List
import argparse
import httpx  # Use httpx for async requests

with open('config.json') as f:
    CONFIG = json.load(f)

partition = {1: (1, 1001), 2: (1001, 2001), 3: (2001, 3001)}

class BankServer:
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
        self.router.add_api_route('/', self.root, methods=['GET'])
        self.router.add_api_route('/balance/{client_id}', self.balance, methods=['POST'])
        self.router.add_api_route('/Hbalance/{client_id}', self.Hbalance, methods=['POST'])

    def activation(self):
        server_num = self.port - CONFIG['HOST_PORT']
        if (server_num) in partition:
            self.accounts.extend(Account(id=i) for i in range(*partition[server_num]))

    def prompt(self):
        """Prompt for commands"""
        time.sleep(1)
        print('Welcome to the blockchain bank server!')
        print('Commands:')
        print('  1. exit (or quit, q)')
        print('Enter a command:')

    def interact(self):
        """Interact with the server"""
        while True:
            cmd = input('>>> ').strip()
            if cmd in ['exit', 'quit', 'q']:
                print('Exiting...')
                os._exit(0)
            elif cmd == 'clients':
                print(self.clients)
            else:
                print('Invalid command')
            print()

    # for fastapi route part
    async def root(self):
        return {'message': 'Welcome to the blockchain bank server!'}

    async def balance(self, client_id: int):
        account = [account for account in self.accounts if account.id == client_id][0]
        account.recent_access_time = get_current_time()
        print("Client {}, Balance {}".format(client_id, account.balance))
        return {'result': 'success'}

    async def Hbalance(self, client_id: int): # handle balance request from user
        """Get the balance of a client"""
        for server_id, (start_id, end_id) in partition.items():
            if start_id <= client_id <= (end_id - 1):
                async with httpx.AsyncClient() as client:
                    server_address = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], 8000 + server_id)
                    res = await client.post(f"{server_address}/balance/{client_id}")
                    return res.json()  # Return the response from the server

        return {'result': 'success'}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Python client example with port argument.")
    parser.add_argument("-p", "--port", type=int, required=True, help="Port number to connect to")
    args = parser.parse_args()

    server = BankServer()
    server.port = args.port + CONFIG['HOST_PORT']
    print(f"server port {server.port}")

    server.activation()
    app = fastapi.FastAPI()
    app.include_router(server.router)
    threading.Thread(target=uvicorn.run, kwargs={
        'app': app,
        'host': server.ipv4,
        'port': server.port,
        'log_level': 'warning'
    }).start()

    server.prompt()
    server.interact()
