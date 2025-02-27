from client import Client
from utils import Endpoint


if __name__ == "__main__":
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", type=str, help="path to the configuration file")
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

    # setup logging
    import logging
    logging.basicConfig(
        level=logging.INFO,
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
        logger=logger
    )
    client.start()
    # read commands from stdin
    while True:
        try:
            cluster = int(input("Cluster: "))
            command = json.loads(input("Command: "))
            response = client.request(cluster, command)
            print(response)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(e)
            continue