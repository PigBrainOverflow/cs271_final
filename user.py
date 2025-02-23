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
        self.command = {
            "b": self.balance,
            "t": self.transfer,
            "tl": self.txnlog,
            "p": self.profiling,
            "q": self.exit
        }

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
            try:
                parts = cmd.split(maxsplit=1)  # Split into command and rest
                func = parts[0]  # First word is the command
                arg = parts[1] if len(parts) > 1 else None  # Argument is optional
                self.command[func](arg)
            except IndexError:
                print("Error: wrong command")
            print()

    def prompt(self):
        """Prompt the user for input"""
        time.sleep(1)
        print('Command: Usage')
        print('  1. balance: b <item-id>')
        print('  2. transfer: t <file path>')
        print('  3. exit: q')
        print('Enter a command:')

    def exit(self, arg=None):
        print('Exiting...')
        process.terminate()
        os._exit(0)

    def balance(self, client_id):
        """Balance transaction via HTTP request"""
        res = requests.post(self.server_addr + '/Hbalance/{}'.format(client_id))

    def transfer(self, file):
        try:
            with open(file, 'r') as file:
                for line in file:
                    (x,y,amt) = tuple(map(lambda v, t: t(v.strip()), line.split(','), (int, int, float)))
                    transaction = Transaction(x=x,y=y,amt=amt)
                    self.transactions.append(transaction)
        except IndexError:
            print("Invalid file path provided.")

        while self.transactions:
            transaction = self.transactions.pop(0)
            res = requests.post(self.server_addr + '/Htransfer', json=transaction.model_dump())

    def txnlog(self, arg=None):
        res = requests.post(self.server_addr + '/Htxnlog')

    def profiling(self, file):
        try:
            with open(file, 'r') as file:
                for line in file:
                    (x,y,amt) = tuple(map(lambda v, t: t(v.strip()), line.split(','), (int, int, float)))
                    transaction = Transaction(x=x,y=y,amt=amt)
                    self.transactions.append(transaction)
        except IndexError:
            print("Invalid file path provided.")

        total_transaction = len(self.transactions)
        start = time.perf_counter()
        while self.transactions:
            transaction = self.transactions.pop(0)
            res = requests.post(self.server_addr + '/Htransfer', json=transaction.model_dump())
        end = time.perf_counter()
        print(f"Throughput: {total_transaction/(end - start)}")

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
