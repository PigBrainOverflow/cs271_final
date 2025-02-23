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
from utils import get_current_time, Transaction
from fastapi import Body
import subprocess

with open('config.json') as f:
    CONFIG = json.load(f)


class User:

    def __init__(self, id, ipv4, port, server_addr) -> None:
        self.id = id
        self.ipv4 = ipv4
        self.port = port
        self.server_addr = server_addr
        self.transactions: List[Transaction] = []

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
                process.terminate()
                os._exit(0)
            elif re.match(r'(balance|bal|b)\s+\d+$', cmd):
                try:
                    id = cmd.split()[1]
                except ValueError:
                    print('Invalid id command')
                    continue
                self.balance(id)
            elif re.match(r'(trans|t)\s+\S+$', cmd):
                try:
                    file = cmd.split(maxsplit=1)[1]  # Extract file path
                    with open(file, 'r') as file:
                        for line in file:
                            (x,y,amt) = tuple(map(lambda v, t: t(v.strip()), line.split(','), (int, int, float)))
                            transaction = Transaction(x=x,y=y,amt=amt)
                            self.transactions.append(transaction)
                except IndexError:
                    print("Invalid file path provided.")

                self.transfer()
            else:
                print('Invalid command')
            print()

    def prompt(self):
        """Prompt the user for input"""
        time.sleep(1)
        print('Commands:')
        print('  1. balance (or bal, b)')
        print('  2. transaction (or trans, t)')
        print('  3. exit (or quit, q)')
        print('Enter a command:')

    def balance(self, client_id):
        """Balance transaction via HTTP request"""
        res = requests.post(self.server_addr + '/Hbalance/{}'.format(client_id))

    def transfer(self):
        while self.transactions:
            transaction = self.transactions.pop(0)
            res = requests.post(self.server_addr + '/Htransfer', json=transaction.model_dump())

if __name__ == '__main__':
    app = fastapi.FastAPI()
    process = subprocess.Popen(
        ["python", "client.py", "-p", "11"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    server_address = 'http://{}:{}'.format(CONFIG['HOST_IPv4'], CONFIG['HOST_PORT'] + 11)
    client = User(10, CONFIG['HOST_IPv4'], port=CONFIG['HOST_PORT'] + 10, server_addr=server_address)
    client.start()
    client.interact()
