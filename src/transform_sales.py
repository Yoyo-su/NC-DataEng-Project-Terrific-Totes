from copy import deepcopy
import pandas as pd
from src.utils.json_to_pd_dataframe import json_to_pd_dataframe
from src.utils.load_data_from_most_recent_json import load_data_from_most_recent_json


def make_fact_sales_order_table(df_sales):

    fact_sales_order = deepcopy(df_sales)

    fact_sales_order['created_date'] = pd.to_datetime(fact_sales_order['created_at']).dt.date.astype(str)
    fact_sales_order['created_time'] = pd.to_datetime(fact_sales_order['created_at']).dt.time.astype(str)

    fact_sales_order['last_updated_date'] = pd.to_datetime(fact_sales_order['last_updated']).dt.date.astype(str)
    fact_sales_order['last_updated_time'] = pd.to_datetime(fact_sales_order['last_updated']).dt.time.astype(str)
    fact_sales_order['sales_staff_id'] = fact_sales_order['staff_id']

    fact_sales_order.drop(columns=['created_at', 'last_updated','staff_id'], inplace=True)

    new_column_order = [
        'sales_order_id',
        'created_date',
        'created_time',
        'last_updated_date',
        'last_updated_time',
        'sales_staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'design_id',
        'agreed_payment_date',
        'agreed_delivery_date',
        'agreed_delivery_location_id'
    ]


    fact_sales_order = fact_sales_order[new_column_order]

    return fact_sales_order


def make_dim_date(fact_sales_order):
    created_date = pd.Series(fact_sales_order['created_date'], name='date_id')
    last_updated_date = pd.Series(fact_sales_order['last_updated_date'], name='date_id')
    agreed_payment_date = pd.to_datetime(fact_sales_order['agreed_payment_date'], errors='coerce')
    agreed_delivery_date = pd.to_datetime(fact_sales_order['agreed_delivery_date'], errors='coerce')

    all_the_dates = pd.concat([
        created_date.astype(str),
        last_updated_date.astype(str),
        pd.Series(agreed_payment_date, name='date_id').astype(str),
        pd.Series(agreed_delivery_date, name='date_id').astype(str)
    ], ignore_index=True).drop_duplicates().dropna().reset_index(drop=True)


    dim_date = pd.DataFrame()
    dim_date['date_id'] = pd.to_datetime(all_the_dates)
    dim_date['year'] = dim_date['date_id'].dt.year
    dim_date['month'] = dim_date['date_id'].dt.month
    dim_date['day'] = dim_date['date_id'].dt.day
    dim_date['day_of_week'] = dim_date['date_id'].dt.dayofweek
    dim_date['day_name'] = dim_date['date_id'].dt.day_name()
    dim_date['month_name'] = dim_date['date_id'].dt.month_name()
    dim_date['quarter'] = dim_date['date_id'].dt.quarter

    return dim_date












