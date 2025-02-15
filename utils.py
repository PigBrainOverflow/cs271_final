import socket
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic import validator


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

    class Config:
        schema_extra = {
            "example": {
                'id': 1,
                'balance': 10.0,
                'recent_access_time': '2023-01-26T15:54'
            }
        }
