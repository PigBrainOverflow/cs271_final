from __future__ import annotations
import asyncio
import random
import json

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

    def __init__(self, _server: Server):
        super().__init__(_server)


    def commit(self):
        raise NotImplementedError


class LeaderPolicy(GeneralPolicy):
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


    async def _handle_election_timeout(self):
        try:
            await asyncio.sleep(random.uniform(self._server._ELECTION_TIMEOUT_MIN, self._server._ELECTION_TIMEOUT_MAX) / 1000.0)
            self._server._logger.info("Election timeout elapsed")
            await self._server._queue.put({"type": "ElectionTimeout"})  # put the message into the queue, notifies the server
        except asyncio.CancelledError:
            pass


    def _reset_election_timeout(self):
        if self._election_timeout_task is not None:
            self._election_timeout_task.cancel()
        self._election_timeout_task = asyncio.create_task(self._handle_election_timeout())


    async def _handle_append_entries(self, message: dict):
        # does not reset election timeout here
        receive_from, content = message["from"], message["content"]
        term = content["term"]
        success = True
        if term < self._server._storage.current_term:
            success = False
        else:
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
                # candidate's log is up-to-date
                self._server._storage.voted_for = candidate_id
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
        # cancel election timeout and switch to candidate if vote granted
        if vote_granted:
            if self._election_timeout_task is not None:
                self._election_timeout_task.cancel()
            return CandidatePolicy(self._server, vote_for_self=False)
        return self


    async def handle_event(self, message: dict) -> Policy:
        type = message["type"]
        if type == "AppendEntriesRPC":
            self._reset_election_timeout()
            await self._handle_append_entries(message)
            return self
        elif type == "RequestVoteRPC":
            return await self._handle_request_vote(message)
        elif type == "ElectionTimeout":
            return CandidatePolicy(self._server, vote_for_self=True)
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

    def __init__(self, _server: Server):
        super().__init__(_server)