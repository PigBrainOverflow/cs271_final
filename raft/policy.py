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


    def commit(self):
        raise NotImplementedError


class LeaderPolicy(Policy):
    """
    1. Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts;
    2. If command received from client: append entry to local log, respond after entry applied to state machine;
    3. If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex;
    4. If successful: update nextIndex and matchIndex for follower;
    5. If AppendEntries fails because of log inconsistency: decrement nextIndex and retry;
    6. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N.
    """

    _name: str = "Leader"

    def __init__(self, _server: Server):
        super().__init__(_server)


class FollowerPolicy(GeneralPolicy):
    """
    1. Respond to RPCs from candidates and leaders;
    2. If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.
    """

    _name: str = "Follower"
    _leader_id: int | None
    _election_timeout_task: asyncio.Task | None

    def __init__(self, _server: Server, leader_id: int | None = None):
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
            if entry is None or entry[0] != prev_log_term:  # log inconsistency
                success = False
            else:
                # append entries
                conflict_found = False
                for entry in content["entries"]:
                    index, term, command = entry["index"], entry["term"], entry["command"]
                    if not conflict_found:
                        entry_in_log = self._server._storage[index]
                        if entry_in_log is None or entry_in_log[0] != term:
                            # find the first conflicting entry
                            conflict_found = True
                            self._server._storage.remove_back(index)
                            self._server._storage.append(index, term, command)
                    else:
                        self._server._storage.append(index, term, command)
                # update commit index
                leader_commit, last_new_index = content["leader_commit"], content["entries"][-1]["index"]
                if leader_commit > self._server._commit_index:
                    self._server._commit_index = min(leader_commit, last_new_index)
                self.commit()
        # send response
        response = {
            "to": receive_from,
            "content": {
                "type": "AppendEntriesResponse",
                "term": self._server._storage.current_term,
                "success": success
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()


    async def _handle_request_vote(self, message: dict) -> Policy:
        receive_from, content = message["from"], message["content"]
        term, candidate_id, last_log_index, last_log_term = content["term"], content["candidate_id"], content["last_log_index"], content["last_log_term"]
        vote_granted = True
        if term < self._server._storage.current_term:
            # from an old candidate, reject
            vote_granted = False
        elif self._server._storage.voted_for is None or self._server._storage.voted_for == candidate_id:
            # from a new candidate
            last_index = len(self._server._storage)
            last_entry = self._server._storage[last_index]
            last_term = last_entry[0] if last_entry is not None else 0
            if last_log_term < last_term or (last_log_term == last_term and last_log_index < last_index):
                # candidate's log is not up-to-date
                vote_granted = False
        else:
            # already voted for another candidate
            vote_granted = False
        # update current term
        self._server._storage.current_term = max(self._server._storage.current_term, term)
        # send response
        response = {
            "to": receive_from,
            "content": {
                "type": "RequestVoteResponse",
                "term": self._server._storage.current_term,
                "vote_granted": vote_granted
            }
        }
        self._server._writer.write(json.dumps(response).encode() + b"\n")
        self._server._logger.info(f"Sent {response} to {receive_from}")
        await self._server._writer.drain()
        # update voted for and reset election timeout if vote granted
        if vote_granted:
            self._server._storage.voted_for = candidate_id
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
            # it will also cancel the election timeout if vote granted
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
        # convert to follower
        follower = FollowerPolicy(self._server, message["content"]["leader_id"])
        follower._reset_election_timeout()
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
                return LeaderPolicy(self._server)
        return self


    async def handle_event(self, message) -> Policy:
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
            # TODO: handle request vote
            return self
        else:
            # ignore other messages
            # TODO: handle client requests
            return self