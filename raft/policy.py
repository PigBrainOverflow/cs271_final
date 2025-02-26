from __future__ import annotations
import asyncio
import random
import json

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .server import Server


# Base class for all policies
class Policy:
    _server: Server

    def __init__(self, _server: Server):
        self._server = _server  # does not own it


    def apply(self) -> list[dict]:
        # apply log entries to state machine from last_applied + 1 to commit_index
        # update last_applied and log results
        # return the list of results
        return self._server.apply()


    async def _process_request_vote(self, message: dict) -> tuple[bool, bool]:
        # process the request vote message
        # return (vote_granted, higher_term)
        receive_from, content = message["from"], message["content"]
        term, candidate_id, last_log_index, last_log_term = content["term"], content["candidate_id"], content["last_log_index"], content["last_log_term"]
        vote_granted, higher_term = False, False    # reject if lower term
        if term >= self._server._storage.current_term:
            if term > self._server._storage.current_term:
                # from a higher term candidate
                higher_term = True
                self._server._storage.current_term = term
                self._server._storage.voted_for = None
            if self._server._storage.voted_for is None or self._server._storage.voted_for == candidate_id:
                # from a new candidate
                last_index = len(self._server._storage)
                last_entry = self._server._storage[last_index]
                last_term = 0 if last_entry is None else last_entry[0]
                if last_log_term > last_term or (last_log_term == last_term and last_log_index >= last_index):
                    # candidate's log is up-to-date
                    vote_granted = True
                    self._server._storage.voted_for = candidate_id
        # send response
        response = {
            "to": receive_from,
            "content": {
                "type": "RequestVoteResponse",
                "term": self._server._storage.current_term,
                "vote_granted": vote_granted,
                "voter_id": self._server._index
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()
        return vote_granted, higher_term


    async def handle_event(self, message: dict) -> Policy:
        raise NotImplementedError


class GeneralPolicy(Policy):
    """
    1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine;
    2. If RPC request or response contains termT > currentTerm: set currentTerm = T,convert to follower.
    """
    _election_timeout_task: asyncio.Task | None

    def __init__(self, _server: Server):
        super().__init__(_server)
        self._election_timeout_task = None


    def __del__(self):
        if self._election_timeout_task is not None:
            self._election_timeout_task.cancel()


    async def _handle_election_timeout(self):
        try:
            await asyncio.sleep(random.uniform(self._server._ELECTION_TIMEOUT_MIN, self._server._ELECTION_TIMEOUT_MAX) / 1000.0)
            self._server._logger.info("Election timeout elapsed")
            # put the message into the queue, notifies the server
            await self._server._queue.put({
                "from": self._server._self_ep.to_dict(),
                "content": {
                    "type": "ElectionTimeout"
                }
            })
        except asyncio.CancelledError:
            pass


    def _reset_election_timeout(self):
        if self._election_timeout_task is not None:
            self._election_timeout_task.cancel()
        self._election_timeout_task = asyncio.create_task(self._handle_election_timeout())


class LeaderPolicy(Policy):
    """
    1. Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts;
    2. If command received from client: append entry to local log, respond after entry applied to state machine;
    3. If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex;
    4. If successful: update nextIndex and matchIndex for follower;
    5. If AppendEntries fails because of log inconsistency: decrement nextIndex and retry;
    6. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N.

    This implementation uses a timed loop to send AppendEntries RPCs for both heartbeats and log replication. Client requests append entries to the log, and the loop ensures synchronization by sending all unreplicated entries. If no new entries exist, a heartbeat (empty entry) is sent.
    """

    _name: str = "Leader"
    _heartbeat_task: asyncio.Task | None
    _next_indices: dict[int, int]   # next index to send to each follower
    _match_indices: dict[int, int]  # highest index replicated to each follower

    async def broadcast_append_entries(self, max_entries: int = 1):
        for index, ep in self._server._peer_eps.items():
            next_index = self._next_indices[index]
            entries = []
            # self._server._logger.info(f"next_index: {next_index}, max_entries: {max_entries}, len: {len(self._server._storage) + 1}")
            for i in range(next_index, min(next_index + max_entries, len(self._server._storage) + 1)):
                term, ip, port, serial_number, content, _ = self._server._storage[i]
                entries.append({
                    "index": i,
                    "term": term,
                    "command": {
                        "ip": ip,
                        "port": port,
                        "serial_number": serial_number,
                        "content": content
                    }
                })
            request = {
                "to": ep.to_dict(),
                "content": {
                    "type": "AppendEntriesRPC",
                    "term": self._server._storage.current_term,
                    "leader_id": self._server._index,
                    "prev_log_index": len(self._server._storage),
                    "prev_log_term": self._server._storage[len(self._server._storage)][0] if len(self._server._storage) > 0 else 0,
                    "entries": entries,
                    "leader_commit": self._server._commit_index
                }
            }
            self._server._writer.write(json.dumps(request).encode() + b"\n")
            self._server._logger.info(f"Sent {request} to {ep.to_dict()}")
        await self._server._writer.drain()


    def __init__(self, _server: Server):
        super().__init__(_server)
        self._server._logger.info("Becoming leader")
        self._next_indices = {index: len(_server._storage) + 1 for index in _server._peer_eps.keys()}
        # self._server._logger.info(f"next_indices: {self._next_indices}")
        self._match_indices = {index: 0 for index in _server._peer_eps.keys()}
        self._heartbeat_task = asyncio.create_task(self._handle_heartbeat())


    def __del__(self):
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()


    async def _respond_to_client(self, result: dict):
        client_ip, client_port = result["ip"], result["port"]
        response = {
            "to": {
                "ip": client_ip,
                "port": client_port
            },
            "content": {
                "type": "ClientResponse",
                "serial_number": result["serial_number"],
                "response": result["response"]
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {client_ip}:{client_port}")
        await self._server._writer.drain()


    async def _handle_heartbeat(self):
        # infinitely put heartbeat notifications into the queue
        try:
            while True:
                await asyncio.sleep(self._server._HEARTBEAT_INTERVAL / 1000.0)
                self._server._logger.info("Sending heartbeat")
                # put the message into the queue, notifies the server
                await self._server._queue.put({
                    "from": self._server._self_ep.to_dict(),
                    "content": {
                        "type": "Heartbeat"
                    }
                })
        except asyncio.CancelledError:
            pass


    async def _handle_append_entries(self, message: dict) -> Policy:
        receive_from, content = message["from"], message["content"]
        term = content["term"]
        # step down if higher term discovered
        if term > self._server._storage.current_term:
            self._server._storage.current_term = term
            self._server._storage.voted_for = None
            follower = FollowerPolicy(self._server, content["leader_id"])
            follower._handle_append_entries(message)
            return follower
        # reject the request if lower or equal term
        response = {
            "to": receive_from,
            "content": {
                "type": "AppendEntriesResponse",
                "term": self._server._storage.current_term,
                "success": False,
                "last_index": len(self._server._storage),
                "follower_id": self._server._index
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()
        return self


    async def _handle_request_vote(self, message: dict) -> Policy:
        # convert to follower if higher term discovered
        # reject the request if lower or equal term
        _, higher_term = await self._process_request_vote(message)
        if higher_term:
            return FollowerPolicy(self._server)
        return self


    async def _handle_append_entries_response(self, message: dict) -> Policy:
        content = message["content"]
        term, success, follower_id = content["term"], content["success"], content["follower_id"]
        if success:
            # update next index and match index
            self._next_indices[follower_id] = content["last_index"] + 1
            self._match_indices[follower_id] = content["last_index"]
            # update commit index
            match_indices = sorted(self._match_indices.values())
            new_commit_index = match_indices[len(match_indices) // 2]
            if new_commit_index > self._server._commit_index:
                self._server._commit_index = new_commit_index
                results = self.apply()
                self._server._logger.info(f"Applied {results}")
                # notify the client
                for result in results:
                    await self._respond_to_client(result)
        elif term > self._server._storage.current_term:
            # convert to follower if higher term discovered
            self._server._storage.current_term = term
            self._server._storage.voted_for = None
            return FollowerPolicy(self._server)
        # retry with a lower next index
        else:
            self._next_indices[follower_id] -= 1
        return self


    async def _handle_client_request(self, message: dict):
        # append the command to the local log
        receive_from, content = message["from"], message["content"]
        self._server._storage.append(
            index=len(self._server._storage) + 1,
            term=self._server._storage.current_term,
            client_ip=receive_from["ip"],
            client_port=receive_from["port"],
            serial_number=content["serial_number"],
            command=content["command"]
        )
        # no need to send response here
        # it's handled by the heartbeat


    async def handle_event(self, message: dict) -> Policy:
        type = message["content"]["type"]
        if type == "AppendEntriesRPC":
            return await self._handle_append_entries(message)
        elif type == "RequestVoteRPC":
            return await self._handle_request_vote(message)
        elif type == "AppendEntriesResponse":
            return await self._handle_append_entries_response(message)
        elif type == "Heartbeat":
            await self.broadcast_append_entries(self._server._MAX_ENTRIES_PER_APPEND_ENTRIES)
            return self
        elif type == "ClientRequest":
            await self._handle_client_request(message)
            return self
        else:
            # ignore other messages
            return self


class FollowerPolicy(GeneralPolicy):
    """
    1. Respond to RPCs from candidates and leaders;
    2. If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
    """

    _name: str = "Follower"
    _leader_id: int | None
    _election_timeout_task: asyncio.Task | None

    def __init__(self, _server: Server, leader_id: int | None = None):
        # include reset election timeout
        super().__init__(_server)
        self._leader_id = leader_id
        self._election_timeout_task = None
        self._reset_election_timeout()


    async def _handle_append_entries(self, message: dict):
        # does not reset election timeout here
        receive_from, content = message["from"], message["content"]
        term = content["term"]
        success = True
        if term < self._server._storage.current_term:
            success = False
        else:
            # update leader id
            self._leader_id = content["leader_id"]
            # update current term and voted for
            if term > self._server._storage.current_term:
                self._server._storage.voted_for = None
            self._server._storage.current_term = term
            # check prev log
            prev_log_index, prev_log_term = content["prev_log_index"], content["prev_log_term"]
            entry = self._server._storage[prev_log_index]
            if (entry is None or entry[0] != prev_log_term) and prev_log_index != 0:    # log is inconsistent
                success = False
            else:
                # append entries
                conflict_found = False
                for entry in content["entries"]:
                    index, term, command = entry["index"], entry["term"], entry["command"]
                    ip, port, serial_number, command_content = command["ip"], command["port"], command["serial_number"], command["content"]
                    if not conflict_found:
                        entry_in_log = self._server._storage[index]
                        if entry_in_log is None or entry_in_log[0] != term:
                            # find the first conflicting entry
                            conflict_found = True
                            self._server._storage.remove_back(index)
                            self._server._storage.append(index, term, ip, port, serial_number, command_content)
                    else:
                        self._server._storage.append(index, term, ip, port, serial_number, command_content)
                # update commit index
                if len(content["entries"]) > 0: # not a heartbeat
                    leader_commit, last_new_index = content["leader_commit"], content["entries"][-1]["index"]
                    if leader_commit > self._server._commit_index:
                        self._server._commit_index = min(leader_commit, last_new_index)
                    self.apply()
        # send response
        response = {
            "to": receive_from,
            "content": {
                "type": "AppendEntriesResponse",
                "term": self._server._storage.current_term,
                "success": success,
                "last_index": len(self._server._storage),
                "follower_id": self._server._index
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()


    async def _handle_request_vote(self, message: dict) -> Policy:
        vote_granted, _ = await self._process_request_vote(message)
        # reset election timeout if vote granted
        if vote_granted:
            self._reset_election_timeout()
        return self


    async def _handle_client_request(self, message: dict):
        # tell the client current leader
        receive_from = message["from"]
        response = {
            "to": receive_from,
            "content": {
                "type": "ClientResponse",
                "leader_id": self._leader_id
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()


    async def handle_event(self, message: dict) -> Policy:
        # return the next policy
        type = message["content"]["type"]
        if type == "AppendEntriesRPC":
            self._reset_election_timeout()
            await self._handle_append_entries(message)
            return self
        elif type == "RequestVoteRPC":
            # it will also reset election timeout if vote granted
            return await self._handle_request_vote(message)
        elif type == "ElectionTimeout":
            # start a new election and vote for self
            candidate = CandidatePolicy(self._server)
            await candidate.broadcast_request_vote()
            return candidate
        elif type == "ClientRequest":
            await self._handle_client_request(message)
            return self
        else:
            # ignore other messages
            return self


class CandidatePolicy(GeneralPolicy):
    """
    1. On conversion to candidate: start election:
        Increment currentTerm;
        Vote for self;
        Reset election timer;
        Send RequestVote RPCs to all other servers;
    2. If votes received from majority of servers: become leader;
    3. If AppendEntries RPC received from new leader: convert to follower;
    4. If election timeout elapses: start new election.
    """

    _name: str = "Candidate"
    _votes: set[int]    # set of servers that voted for this candidate

    async def broadcast_request_vote(self):
        last_log_index = len(self._server._storage)
        request_content = {
            "type": "RequestVoteRPC",
            "term": self._server._storage.current_term,
            "candidate_id": self._server._index,
            "last_log_index": last_log_index,
            "last_log_term": self._server._storage[last_log_index][0] if last_log_index > 0 else 0
        }
        # send request vote to all other servers
        for ep in self._server._peer_eps.values():
            request = {
                "to": ep.to_dict(),
                "content": request_content
            }
            self._server._writer.write(json.dumps(request).encode() + b"\n")
            self._server._logger.info(f"Sent {request} to {ep.to_dict()}")
        await self._server._writer.drain()


    def _init(self):
        self._votes = set()
        self._server._storage.current_term += 1
        self._server._storage.voted_for = self._server._index
        self._reset_election_timeout()


    def __init__(self, _server: Server):
        super().__init__(_server)
        self._init()


    async def _handle_append_entries(self, message: dict) -> Policy:
        receive_from, content = message["from"], message["content"]
        term = content["term"]
        if term < self._server._storage.current_term:
            # reject the request if lower term
            response = {
                "to": receive_from,
                "content": {
                    "type": "AppendEntriesResponse",
                    "term": self._server._storage.current_term,
                    "success": False,
                    "last_index": len(self._server._storage),
                    "follower_id": self._server._index
                }
            }
            self._server._writer.write(json.dumps(response).encode() + b"\n")
            self._server._logger.info(f"Sent {response} to {receive_from}")
            await self._server._writer.drain()
            return self
        # convert to follower
        follower = FollowerPolicy(self._server, message["content"]["leader_id"])
        await follower._handle_append_entries(message)
        return follower


    async def _handle_request_vote_response(self, message: dict) -> Policy:
        content = message["content"]
        term, vote_granted = content["term"], content["vote_granted"]
        if term > self._server._storage.current_term:
            # convert to follower if higher term discovered
            self._server._storage.current_term = term
            self._server._storage.voted_for = None
            # this follower does not know the new leader yet
            return FollowerPolicy(self._server)
        elif term == self._server._storage.current_term and vote_granted:
            # vote granted
            self._votes.add(content["voter_id"])
            if len(self._votes) + 1 > (len(self._server._peer_eps) + 1) // 2:
                # convert to leader
                leader = LeaderPolicy(self._server)
                await leader.broadcast_append_entries()   # send initial heartbeat
                return leader
        return self


    async def _handle_request_vote(self, message: dict) -> Policy:
        # does not reset election timeout here
        receive_from, content = message["from"], message["content"]
        term = content["term"]
        if term > self._server._storage.current_term:
            # convert to follower if higher term discovered
            self._server._storage.current_term = term
            self._server._storage.voted_for = content["candidate_id"]
            response = {
                "to": receive_from,
                "content": {
                    "type": "RequestVoteResponse",
                    "term": self._server._storage.current_term,
                    "vote_granted": True,
                    "voter_id": self._server._index
                }
            }
            self._server._writer.write(json.dumps(response).encode() + b"\n")
            self._server._logger.info(f"Sent {response} to {receive_from}")
            await self._server._writer.drain()
            return FollowerPolicy(self._server)
        return self # ignore the request


    async def _handle_client_request(self, message: dict):
        # tell the client current leader
        receive_from = message["from"]
        response = {
            "to": receive_from,
            "content": {
                "type": "ClientResponse",
                "leader_id": None
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()


    async def handle_event(self, message: dict) -> Policy:
        type = message["content"]["type"]
        if type == "AppendEntriesRPC":
            # convert to follower
            return await self._handle_append_entries(message)
        elif type == "RequestVoteResponse":
            # convert to leader if majority votes received
            # convert to follower if higher term discovered
            return await self._handle_request_vote_response(message)
        elif type == "ElectionTimeout":
            # start a new election and vote for self
            self._init()
            await self.broadcast_request_vote()
            return self
        elif type == "RequestVoteRPC":
            return await self._handle_request_vote(message)
        elif type == "ClientRequest":
            await self._handle_client_request(message)
            return self