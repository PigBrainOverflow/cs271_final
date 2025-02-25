from raft.server import Server

if __name__ == "__main__":
    import sys
    import logging

    logging.basicConfig(level=logging.INFO)
    index = int(sys.argv[1])
    self_ep = Endpoint(*sys.argv[2].split(":"))
    router_ep = Endpoint(*sys.argv[3].split(":"))
    peer_eps = [Endpoint(*ep.split(":")) for ep in sys.argv[4:]]
    server = Server(index, self_ep, router_ep, peer_eps, logging.getLogger())
    server.start()