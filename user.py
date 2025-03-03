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
import asyncio
import httpx
import random

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
        for process in processes:
            process.terminate()
        os._exit(0)

    def balance(self, client_id):
        """Balance transaction via HTTP request"""
        res = requests.post(random.choice(self.server_addr) + '/Hbalance/{}'.format(client_id))

    def transfer(self, file):
        try:
            with open(file, 'r') as file:
                for line in file:
                    (x,y,amt) = tuple(map(lambda v, t: t(v.strip()), line.split(','), (int, int, float)))
                    transaction = Transaction(x=x,y=y,amt=amt)
                    self.transactions.append(transaction)
        except IndexError:
            print("Invalid file path provided.")

        asyncio.run(self.process_trans())

    def txnlog(self, arg=None):
        res = requests.post(random.choice(self.server_addr) + '/Htxnlog')

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
        results = asyncio.run(self.process_trans(prof=True))
        end = time.perf_counter()
        print(f"Throughput: {total_transaction/(end - start)}")
        print(f"Latency:")
        for result in results:
                print(f"    {result:.3f} seconds")

    async def process_single_trans(self, client: httpx.AsyncClient, transaction: Transaction, server_idx: int):
        addr = self.server_addr[server_idx % len(self.server_addr)]
        start_time = time.perf_counter()
        response = await client.post(
            addr + '/Htransfer',
            json=transaction.model_dump()
        )
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        return elapsed_time

    async def process_trans(self,prof=False):
        async with httpx.AsyncClient() as client:
            # Create tasks for all transactions
            if prof == False:
                tasks = [
                    client.post(random.choice(self.server_addr) + '/Htransfer', json=transaction.model_dump())
                    for transaction in self.transactions
                ]
                responses = await asyncio.gather(*tasks)
                self.transactions.clear()
                for res in responses:
                    print(res.json())
                return None
            else:
                tasks = [
                    self.process_single_trans(client, transaction, idx)
                    for idx, transaction in enumerate(self.transactions)
                ]
                results = await asyncio.gather(*tasks)
                self.transactions.clear()
                return results

if __name__ == '__main__':
    app = fastapi.FastAPI()

    processes = []
    for i in range(11, 15):
        process = subprocess.Popen(
            ["python", "client.py", "-p", str(i)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        processes.append(process)

    server_address = [
        'http://{}:{}'.format(CONFIG['HOST_IPv4'], CONFIG['HOST_PORT'] + i)
        for i in range(11, 15)
    ]

    client = User(10, CONFIG['HOST_IPv4'], port=CONFIG['HOST_PORT'] + 10, server_addr=server_address)
    client.start()
    client.interact()
