from obsloctap.schedule24h import Schedule24

default_fn = "schedule24rs.pkl"


def store_schedule_file(fn: str = default_fn) -> None:
    sp = Schedule24()
    visits = sp.get_schedule24()
    visits.to_pickle(fn)


if __name__ == "__main__":
    store_schedule_file()
