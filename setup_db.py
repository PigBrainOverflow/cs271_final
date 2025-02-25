if __name__ == "__main__":
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", type=str, help="path to the configuration file")
    args = parser.parse_args()

    import json
    from utils import PersistentStorage
    with open(args.config) as f:
        config = json.load(f)

    # setup db
    for cluster in config["clusters"]:
        for member in cluster["members"]:
            db_name = f"server{member['index']}"
            storage = PersistentStorage(db_name)
            storage.create_tables()
            storage.init_tables()