import asyncio
import logging
import json

from utils import Endpoint


class Router:
    # NOTE: Router depends on b"\n" as a message delimiter. Don't use it in message content!

    _listen_ep: Endpoint # Endpoint to listen on
    _user_ep: Endpoint   # Endpoint to connect to the user interface
    _connections: dict[Endpoint, asyncio.StreamWriter]
    _crashed_connections: set[Endpoint] # Set of (temporarily) crashed connections
    _server: asyncio.Server
    _logger: logging.Logger


    def __init__(self, listen_ep: Endpoint, user_ep: Endpoint, logger: logging.Logger = None):
        self._listen_ep = listen_ep
        self._user_ep = user_ep
        self._connections = {}
        self._crashed_connections = set()
        self._server = None
        self._logger = logger


    async def _handle_terminate(self):
        self._logger.info("Terminating server")
        for writer in self._connections.values():
            writer.close()
        self._server.close()


    def _handle_crash(self, targets: list[Endpoint]):
        self._crashed_connections.update(targets)


    def _handle_recover(self, targets: list[Endpoint]):
        self._crashed_connections.difference_update(targets)


    async def _handle_user(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while data := await reader.readuntil(b"\n"):
                self._logger.info(f"Received {data} from user")
                try:
                    data = json.loads(data.decode())
                except json.JSONDecodeError:
                    self._logger.error("Invalid message from user")
                    continue
                if "command" in data:
                    command = data["command"]
                    if command == "terminate":
                        await self._handle_terminate()
                        break
                    elif command == "crash":
                        try:
                            targets = [Endpoint(*target) for target in data["targets"]]
                        except (KeyError, TypeError):
                            self._logger.error("Invalid message from user")
                            continue
                        self._handle_connection(targets)
                    elif command == "recover":
                        try:
                            targets = [Endpoint(*target) for target in data["targets"]]
                        except (KeyError, TypeError):
                            self._logger.error("Invalid message from user")
                            continue
                        self._handle_recover(targets)
        except asyncio.CancelledError:
            self._logger.info("User connection closed successfully")
        except asyncio.IncompleteReadError:
            self._logger.error("User connection closed unexpectedly")
        finally:
            writer.close()
            await writer.wait_closed()


    async def _handle_others(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, peername: Endpoint):
        try:
            while data := await reader.readuntil(b"\n"):
                self._logger.info(f"Received {data} from {peername}")
                if peername in self._crashed_connections:
                    self._logger.info(f"Failed to receive {data} from {peername}")
                    continue
                try:
                    data = json.loads(data.decode())
                except json.JSONDecodeError:
                    self._logger.error(f"Invalid message from {peername}")
                    continue
                if "to" in data and "content" in data:
                    to, content = data["to"], data["content"]
                    to_ep = Endpoint(to["ip"], to["port"])
                    if to_ep in self._connections:
                        if to_ep in self._crashed_connections:
                            self._logger.info(f"Failed to send {content} to {to_ep}")
                        else:
                            self._connections[to_ep].write(json.dumps({"from": peername.to_dict(), "content": content}).encode() + b"\n")
                            # await self._connections[to_ep].drain()
                            self._logger.info(f"Sent {content} to {to_ep}")
                    else:
                        self._logger.error(f"Connection to {to_ep} not found")
                else:
                    self._logger.error(f"Invalid message from {peername}")
        except asyncio.CancelledError:
            self._logger.info(f"Connection from {peername} closed successfully")
        except asyncio.IncompleteReadError:
            self._logger.info(f"Connection from {peername} closed by peer")
        finally:
            writer.close()
            await writer.wait_closed()


    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # Accept a new connection
        peername = Endpoint(*writer.get_extra_info("peername"))
        self._logger.info(f"Connection from {peername}")
        self._connections[peername] = writer

        if peername == self._user_ep:
            await self._handle_user(reader, writer)

        else:
            await self._handle_others(reader, writer, peername)


    async def _accept(self):
        self._server = await asyncio.start_server(self._handle_connection, self._listen_ep.ip, self._listen_ep.port)
        self._logger.info(f"Listening on {self._listen_ep}")
        try:
            async with self._server:
                await self._server.serve_forever()
        except asyncio.CancelledError:
            self._logger.info("Server closed successfully")


    async def async_start(self):
        await self._accept()


    def start(self):
        try:
            asyncio.run(self._accept())
        except KeyboardInterrupt:
            self._logger.info("Received keyboard interrupt")