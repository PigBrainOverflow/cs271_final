import os
import re
import json
import time
import fastapi
import uvicorn
import logging
import requests
import warnings
import threading
import timeloop
from typing import List
from datetime import timedelta
from utils import get_current_time
from fastapi import Body

with open('config.json') as f:
    CONFIG = json.load(f)


class User:

    def __init__(self, id, ipv4, port, server_addr) -> None:
        self.id = id
        self.ipv4 = ipv4
        self.port = port
        self.server_addr = server_addr

    def start(self):
        """Start the client (FastAPI Web application)"""
        threading.Thread(target=uvicorn.run, kwargs={
            'app': app,
            'host': self.ipv4,
            'port': self.port,
            'log_level': 'warning'
        }).start()

    def interact(self):
        """Begin user interacting"""
        self.prompt()
        while True:
            cmd = input('>>> ').strip().lower()
            if re.match(r'(exit|quit|q)$', cmd):
                print('Exiting...')
                os._exit(0)
            elif re.match(r'(balance|bal|b)\s+\d+$', cmd):
                try:
                    id = cmd.split()[1]
                except ValueError:
                    print('Invalid id command')
                    continue
                self.balance(id)
            else:
                print('Invalid command')
            print()

    def prompt(self):
        """Prompt the user for input"""
        time.sleep(1)
        print('Commands:')
        print('  1. balance (or bal, b)')
        print('  2. exit (or quit, q)')
        print('Enter a command:')

    def balance(self, client_id):
        """Balance transaction via HTTP request"""
        res = requests.get(self.server_addr + '/balance/{}'.format(client_id))
        assert res.status_code == 200
        print("Client {}, Balance {}".format(client_id, res.json()['balance']))

if __name__ == '__main__':
    app = fastapi.FastAPI()
    server_address = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], CONFIG['HOST_PORT'])
    client = User(10, CONFIG['HOST_IPv4'], port=CONFIG['HOST_PORT'] + 10, server_addr=server_address)
    client.start()
    client.interact()
