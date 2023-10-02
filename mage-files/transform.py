import polars as pl
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def get_dim_date(data,  *args, **kwargs):
    datetime_dim = data.select(
        pl.col("tpep_pickup_datetime"),
        pl.col('tpep_pickup_datetime').dt.hour().alias('pickup_hour'),
        pl.col('tpep_pickup_datetime').dt.day().alias('pickup_day'),
        pl.col('tpep_pickup_datetime').dt.weekday().alias('pickup_weekday'),
        pl.col('tpep_pickup_datetime').dt.month().alias('pickup_month'),
        pl.col('tpep_pickup_datetime').dt.year().alias('pickup_year'),
        pl.col("tpep_dropoff_datetime"),
        pl.col('tpep_dropoff_datetime').dt.hour().alias('dropoff_hour'),
        pl.col('tpep_dropoff_datetime').dt.day().alias('dropoff_day'),
        pl.col('tpep_dropoff_datetime').dt.weekday().alias('dropoff_weekday'),
        pl.col('tpep_dropoff_datetime').dt.month().alias('dropoff_month'),
        pl.col('tpep_dropoff_datetime').dt.year().alias('dropoff_year')
    ).unique().with_row_count('datetime_id')
    return datetime_dim

def get_passenger_dim(data, *args, **kwargs):
    passengers = data.select(
        pl.col('passenger_count')
    ).unique().with_row_count('passenger_count_id')
    return passengers

def get_rate_code_dim(data, *args, **kwargs):
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }
    rate_code_dim = data.select(
        pl.col("RatecodeID"),
        pl.col("RatecodeID").map_dict(rate_code_type).alias("rate_code_name")
    ).unique().with_row_count('rate_code_id')
    return rate_code_dim

def get_payment_dim(data, *args, **kwargs):
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_type_dim = data.select(
        pl.col("payment_type"),
        pl.col("payment_type").map_dict(payment_type_name).alias("payment_type_name")
    ).unique().with_row_count('payment_type_id')
    return payment_type_dim

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = pl.DataFrame(data)
    uber_data = df.with_columns(
        pl.col('tpep_pickup_datetime').str.to_datetime().alias('tpep_pickup_datetime'),
        pl.col('tpep_dropoff_datetime').str.to_datetime().alias('tpep_dropoff_datetime'),
    ).with_row_count('trip_id')

    date_dim = get_dim_date(uber_data)
    passenger_dim = get_passenger_dim(uber_data)
    rate_dim = get_rate_code_dim(uber_data)
    payment_type_dim = get_payment_dim(uber_data)

    # Slight mod from eaxmple. Dropped Longitude and Latitude
    fact_rides = uber_data.join(date_dim, on = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
        .join(passenger_dim, on = 'passenger_count') \
        .join(rate_dim, on = 'RatecodeID')  \
        .join(payment_type_dim, on = 'payment_type') \
        .select(['trip_id','VendorID', 'datetime_id', 'passenger_count_id','payment_type_id', 
                'rate_code_id', 'store_and_fwd_flag', 
                # location can be stored in a separate dimension however for simplicity and improved transformation was kept as part of the fact
                'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude',
                'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount'])

    data_model_dict = {
        'fact_rides': fact_rides.to_dict(as_series=False), 
        'date_dim': date_dim.to_dict(as_series=False), 
        'passenger_dim': passenger_dim.to_dict(as_series=False),
        'rate_code_dim': rate_dim.to_dict(as_series=False),
        'payment_type_dim': payment_type_dim.to_dict(as_series=False)
    }
    return data_model_dict


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'