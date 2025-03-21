## Distributed Banking System

This is the final project of CS271 at UCSB. See attached document for more information.

## How to run the project
1. run `launch_all.py` to start the servers and a router;
2. run `launch_client.py` to start a client;
3. run `launch_user.py` to start a user.

## Design
### Router
The router is a simple relay server that forwards messages for the clients and servers. A user can directly connect to the router and send commands to it. `crash` command can be used to simulate disconnected servers. `recover` command can be used to recover the crashed servers.

### Server
Servers are simply implemented by Raft consensus algorithm. Each server has a `balance_table` and a `lock_table` as its state machine.

### Client
Clients serve as coordinators for the clusters. We assume the clients are reliable and will not crash during transactions.

### Transaction
The transaction is implemented by two-phase commit and two-phase locking. The client will first try to acquire locks for target items. If all locks are acquired, it will query the servers to check if a transaction can be accepted. If both servers agree, the client will send a `commit` message to the servers. Otherwise, it will send an `abort` message. To prevent deadlocks, we always acquire locks of items in the increasing order of their ids and release them in the decreasing order.
