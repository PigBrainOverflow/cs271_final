class Client:
    _leaders: dict[int, int | None]    # cluster index -> leader index