import pandas as pd

from generate import FakeUserRegistration
from splitter import splitter

if __name__ == "__main__":
    registrations = FakeUserRegistration()

    registration_data = registrations.get_data()

    df = pd.DataFrame(registration_data)
    df["group"] = df["userid"].apply(
        lambda id: "control" if splitter(id) == 0 else "test"
    )
