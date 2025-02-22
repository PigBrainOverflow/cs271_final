import socket
from router import *
import json


if __name__ == "__main__":
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 8081))
        s.connect(("127.0.0.1", 8080))

        msg = {
            "command": "terminate"
        }
        s.sendall(json.dumps(msg).encode() + b"\n")

        s.shutdown(socket.SHUT_WR)
        while data := s.recv(1024):
            print(data.decode(), end="")
        s.close()
    except:
        print("Connection failed")