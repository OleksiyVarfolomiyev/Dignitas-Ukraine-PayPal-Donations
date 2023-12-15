import pandas as pd
import data_aggregation_tools as da

def format_money(value):
    if abs(value) >= 1e6:
        return '{:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.2f}K'.format(value / 1e3)
    else:
        return '{:.2f}'.format(value)

def format_money_USD(value):
    if abs(value) >= 1e6:
        return '${:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '${:.2f}K'.format(value / 1e3)
    else:
        return '${:.2f}'.format(value)

def read_data():
    dtypes = {
        'Currency': 'str',
        'Category': 'str',
        'Country': 'str',
        'Amount': 'float'
    }
    
    df = pd.read_csv('data/PayPal.csv', dtype=dtypes, parse_dates=['Date'])
    
    df['Date'] = pd.to_datetime(df['Date'])
    df['Category'].fillna('', inplace=True)
    df['Country'].fillna('', inplace=True)

    return df