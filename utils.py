import socket
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic import validator
import json
from colorama import Fore, Style

def get_host_ip():
    """Get host IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


def get_current_time(fmt='%Y-%m-%dT%H:%M:%S'):
    """Get current time in specific string format"""
    return datetime.now().strftime(fmt)


class Account(BaseModel):
    id: int = Field(example=1)
    balance: float = Field(default=10.0)
    recent_access_time: str = None

    @validator('recent_access_time', pre=True, always=True)
    def set_create_time_now(cls, v):
        return v or get_current_time()

    def __str__(self):
        table_header = f"| {'Client':^7} | {'BAL':^7} | {'Timestamp':^30} |"
        table_divider = "-" * len(table_header)
        table_row = f"| {self.id:^7} | {self.balance:^7.2f} | {self.recent_access_time:^30} |"
        return f"{table_divider}\n{table_header}\n{table_divider}\n{table_row}\n{table_divider}"

    def to_json(self):
        return json.dumps(self.dict(), sort_keys=True)

class Transaction(BaseModel):
    x: int = Field(example=1)
    y: int = Field(example=1)
    amt: float = Field(example=1.0)

    def to_json(self):
        return json.dumps(self.__dict__, sort_keys=True)

    def to_tuple(self):
        return self.x, self.y, self.amt

class TxnLog(Transaction):
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    def __str__(self):
        return f"{Fore.YELLOW}<<<{self.timestamp}>>>{Style.RESET_ALL} Transaction from {Fore.GREEN}{str(self.x)}{Style.RESET_ALL} to {Fore.GREEN}{str(self.y)}{Style.RESET_ALL} of amount {Fore.GREEN}{str(self.amt)}{Style.RESET_ALL}"