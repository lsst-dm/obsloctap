import pickle

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
