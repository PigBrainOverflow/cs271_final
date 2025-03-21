from router import Router, Endpoint


if __name__ == "__main__":
    # singleton
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", type=str, help="path to the configuration file")
    parser.add_argument("--loglevel", default="INFO", type=str, help="logging level")
    args = parser.parse_args()

    # load configuration
    import json
    with open(args.config) as f:
        config = json.load(f)
    listen_ep = Endpoint(config["router"]["ip"], config["router"]["port"])
    user_ep = Endpoint(config["user"]["ip"], config["user"]["port"])

    # setup logging
    import logging
    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            # logging.StreamHandler(),
            logging.FileHandler("router.log", mode="w")
        ]
    )
    logger = logging.getLogger("router")

    # start the router
    router = Router(listen_ep, user_ep, logger)
    router.start()