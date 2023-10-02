import io
import polars
import polars as pl
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs) -> polars.DataFrame:
    """
    Template for loading data from API
    """
    url = 'https://storage.googleapis.com/uber_project_ir/uber_data.csv'
    response = requests.get(url)

    return pl.read_csv(io.StringIO(response.text))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'