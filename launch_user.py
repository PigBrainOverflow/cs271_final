from user import User
from utils import Endpoint


if __name__ == "__main__":
    import json
    with open("config.json") as f:
        config = json.load(f)

    user_ep = Endpoint(config["user"]["ip"], config["user"]["port"])
    router_ep = Endpoint(config["router"]["ip"], config["router"]["port"])
    server_eps = {
        member["index"]: Endpoint(member["ip"], member["port"])
        for cluster in config["clusters"]
        for member in cluster["members"]
    }

    user = User(
        self_ep=user_ep,
        router_ep=router_ep,
        server_eps=server_eps
    )
    user.connect_to_router()
    while True:
        try:
            command = str(input(">> "))
            args = command.split()
            type = args[0]
            if type == "crash":
                targets = [int(arg) for arg in args[1:]]
                user.crash(targets)
            elif type == "recover":
                targets = [int(arg) for arg in args[1:]]
                user.recover(targets)
            elif type == "terminate":
                user.terminate()
            elif type == "exit":
                break
        except Exception as e:
            print(e)
            continue
    user.exit()