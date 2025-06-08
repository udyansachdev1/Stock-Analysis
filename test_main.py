# To Test if the Sql connection is working and the data is being pulled

import pandas as pd
import finnhub


def test_connection():
    finnhub_client = finnhub.Client(
            api_key="clla0nhr01qhqdq2t4e0clla0nhr01qhqdq2t4eg"
        )

    res = finnhub_client.quote('AAPL')
    df = pd.DataFrame(res, index=[0])
    assert df.shape[0] > 0


if __name__ == "__main__":
    test_connection()
