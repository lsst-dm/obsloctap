import pickle

# from obsloctap.consumekafka import get_schema, unpack_message
from obsloctap.PredictedSchedule import convert_predicted


def test_convert() -> None:
    # Load data from pickle file for testing
    with open("tests/predicted.pkl", "rb") as f:
        predicted = pickle.load(f)
    predicted.to_parquet("predicted.parquet")
    plan = convert_predicted(predicted)
    count = len(plan)
    assert count == 53  # the rest are 0 time or None.
    print(f"Loaded {count} from pickle.")


def test_msg() -> None:
    # schema = get_schema()
    with open("tests/predicted_message.pkl", "rb") as f:
        msg = pickle.load(f)
    assert msg
    # plan = unpack_message(msg, schema)
    # assert plan
