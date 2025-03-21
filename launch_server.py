from server import Server
from utils import Endpoint


if __name__ == "__main__":
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", type=str, help="path to the configuration file")
    parser.add_argument("--cluster", type=int, help="cluster index")
    parser.add_argument("--member", type=int, help="member index")
    parser.add_argument("--loglevel", default="INFO", type=str, help="log level")
    args = parser.parse_args()

    # load configuration
    import json
    with open("config.json") as f:
        config = json.load(f)
    try:
        cluster = config["clusters"][args.cluster]
        member = cluster["members"][args.member]
        index, self_ep = member["index"], Endpoint(member["ip"], member["port"])
        router_ep = Endpoint(config["router"]["ip"], config["router"]["port"])
        peer_eps = {
            member["index"]: Endpoint(member["ip"], member["port"])
            for member in cluster["members"] if member["index"] != index
        }
        item_range = cluster["item_range"]
        initial_balance = config["initial_balance"]
    except IndexError:
        print("Invalid cluster or member index")
        exit(1)

    # setup logging
    import logging
    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            # logging.StreamHandler(),
            logging.FileHandler(f"server{index}.log", mode="w")
        ]
    )
    logger = logging.getLogger(f"server{index}")

    # start the server
    server = Server(
        index=index,
        self_ep=self_ep,
        router_ep=router_ep,
        peer_eps=peer_eps,
        logger=logger,
        lock_table={i: None for i in range(item_range[0], item_range[1])},
        balance_table={i: initial_balance for i in range(item_range[0], item_range[1])}
    )
    server.start()