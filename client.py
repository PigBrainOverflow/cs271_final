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


with open('config.json') as f:
    CONFIG = json.load(f)


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
        self.router.add_api_route('/balance/{client_id}', self.balance, methods=['GET'])

    def activation(self):
        for i in range(1, 101):
            self.accounts.append(Account(id = i))

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
        """Get the balance of a client"""
        account = [account for account in self.accounts if account.id == client_id][0]
        account.recent_access_time = get_current_time()
        return {'balance': account.balance}


if __name__ == '__main__':
    server = BankServer()
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
