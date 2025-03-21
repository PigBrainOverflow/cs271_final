import time

from client import Client
from utils import Endpoint


if __name__ == "__main__":
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", type=str, help="path to the configuration file")
    parser.add_argument("--loglevel", default="INFO", type=str, help="log level")
    args = parser.parse_args()

    # load configuration
    import json
    with open(args.config) as f:
        config = json.load(f)
    clusters = {
        cluster["index"]: [member["index"] for member in cluster["members"]]
        for cluster in config["clusters"]
    }
    server_eps = {
        member["index"]: Endpoint(member["ip"], member["port"])
        for cluster in config["clusters"]
        for member in cluster["members"]
    }
    router_ep = Endpoint(config["router"]["ip"], config["router"]["port"])
    item_to_cluster = {
        tuple(cluster["item_range"]): cluster["index"]
        for cluster in config["clusters"]
    }

    # setup logging
    import logging
    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            # logging.StreamHandler(),
            logging.FileHandler("client.log", mode="w")
        ]
    )
    logger = logging.getLogger("client")

    # start the client
    client = Client(
        clusters=clusters,
        server_eps=server_eps,
        router_ep=router_ep,
        item_to_cluster=item_to_cluster,
        logger=logger
    )
    client.start()
    # read commands from stdin
    while True:
        try:
            command = str(input(">> "))
            start = time.time()
            args = command.split()
            type = args[0]
            if type == "deposit":
                item_id, amount = int(args[1]), int(args[2])
                response = client.deposit(item_id, amount)
            elif type == "withdraw":
                item_id, amount = int(args[1]), int(args[2])
                response = client.withdraw(item_id, amount)
            elif type == "balance":
                item_id = int(args[1])
                response = client.balance(item_id)
            elif type == "transfer":
                from_id, to_id, amount = int(args[1]), int(args[2]), int(args[3])
                response = client.transfer(from_id, to_id, amount)
            elif type == "exit":
                break
            else:
                logger.error("Invalid command")
                continue
            logger.info(response)
            print(response)
            print(f"Time elapsed: {time.time() - start:.3f}s")
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(e)
            continue