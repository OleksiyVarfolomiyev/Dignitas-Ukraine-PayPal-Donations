import os
import boto3
import json
import pandas as pd
from datetime import datetime
import datetime as dt

def format_money_USD(value):
    if abs(value) >= 1e6:
        return '${:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '${:.2f}K'.format(value / 1e3)
    else:
        return '${:.2f}'.format(value)

def format_money(value):
    if abs(value) >= 1e6:
        return '{:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.2f}K'.format(value / 1e3)
    else:
        return '{:.2f}'.format(value)

def read_PayPal_txs(start_date):
    '''Read only the new PayPal transactions from AWS'''

    access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')

    objects = s3.list_objects(Bucket='finmap-trans')

    transactions = []

    for obj in objects['Contents']:

        if 'trans-paypal' in obj['Key']:
            # Extract the date from the object key
            obj_date_str = '-'.join(obj['Key'].split('-')[2:5]).split('.')[0]
            obj_date = datetime.strptime(obj_date_str, '%Y-%m-%d')
            # Only process the object if its date is greater than or equal to the specified date
            if obj_date >= start_date:
            #if obj['Key'].startswith('trans-paypal'):
                file = s3.get_object(Bucket='finmap-trans', Key=obj['Key'])
                file_content = file['Body'].read().decode('utf-8')

                while file_content:
                    json_content, idx = json.JSONDecoder().raw_decode(file_content)
                    file_content = file_content[idx:].lstrip()

                for item in json_content:
                    transaction_info = item.get('transaction_info', {})
                    payer_info = item.get('payer_info', {})
                    payer_name = payer_info.get('payer_name', {})
                    shipping_info = item.get('shipping_info', {})
                    address = shipping_info.get('address', {})
                    transaction_note = transaction_info.get('transaction_note', '')

                    transaction_data = {
                        'Date': transaction_info.get('transaction_initiation_date'),
                        'FullName': payer_name.get('alternate_full_name'),
                        'Email': payer_info.get('email_address'),
                        'Country': address.get('country_code'),
                        'State': address.get('state'),
                        'City': address.get('city'),
                        'Currency': transaction_info.get('transaction_amount', {}).get('currency_code'),
                        'Gross': transaction_info.get('transaction_amount', {}).get('value'),
                        'Fee': transaction_info.get('fee_amount', {}).get('value'),
                        'Category': transaction_info.get('transaction_subject'),
                        'TransactionNote': transaction_note
                    }
                    transactions.append(transaction_data)

    return pd.DataFrame(transactions)


def etl(df):
    '''Extract, transform, and load the new PayPal transactions,
    append to existing data in PayPal.csv'''

    # ANONIMIZE: Keep only the first name
    df['FullName'] = df['FullName'].str.split().str[0]

    df['Fee'].fillna(0)
    df = df.fillna('')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Gross'] = df['Gross'].astype(float).abs()
    df['Gross'] = df['Gross'].abs()
    df['Fee'] = pd.to_numeric(df['Fee'], errors='coerce').fillna(0).astype(float)
    df['Fee'] = df['Fee'].astype(float)

    df['Amount'] = df['Gross'] + df['Fee']
    df = df.drop(['Gross', 'Fee'], axis=1)
    df['City'] = df['City'].str.title()
    df.loc[df.City == 'Kiev', 'City'] = 'Kyiv'

    mask = df['Category'].str.contains('General', case=False, na=False)
    df.loc[mask, 'Category'] = 'General'
    mask = df['Category'].str.contains('1000', case=False, na=False)
    df.loc[mask, 'Category'] = '1000 Drones for Ukraine'
    mask = df['Category'].str.contains('Milan', case=False, na=False)
    df.loc[mask, 'Category'] = '1000 Drones for Ukraine'
    mask = df['Category'].str.contains('support ukraine', case=False, na=False)
    df.loc[mask, 'Category'] = 'General'
    mask = df['Category'].str.contains('victory', case=False, na=False)
    df.loc[mask, 'Category'] = 'Victory Drones'
    mask = df['Category'].str.contains('flight', case=False, na=False)
    df.loc[mask, 'Category'] = 'Flight to Recovery'
    mask = df['Category'].str.contains('custom', case=False, na=False)
    df.loc[mask, 'Category'] = 'General'
    mask = df['Category'].str.contains('units', case=False, na=False)
    df.loc[mask, 'Category'] = 'Mobile Shower Laundry Units'
    mask = df['Category'].str.contains('shower', case=False, na=False)
    df.loc[mask, 'Category'] = 'Mobile Shower Laundry Units'

    file_path = 'data/PayPal.csv'

    try:
        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)
        else:
            df_existing = pd.DataFrame()
    except pd.errors.EmptyDataError:
        df_existing = pd.DataFrame()
    except FileNotFoundError:
        df_existing = pd.DataFrame()

    # Append new transactions
    df_combined = pd.concat([df_existing, df])

    df_combined['Date'] = pd.to_datetime(df_combined['Date'])

    df_new = df_combined.drop_duplicates(keep='first')

    df_new = df_new.sort_values(by=['Date'], ascending=True)

    df_new.to_csv(file_path, index=False)


def ETL_raw_data(nrows = None):
    '''Extract and transform data and save it to csv files'''

    dtypes = {
        'FullName': 'str',
        'Email': 'str',
        'Country': 'str',
        'State': 'str',
        'City': 'str',
        'Currency': 'str',
        'Amount': 'float',
        'Category': 'str'
    }

    df = pd.read_csv('data/PayPal.csv', dtype=dtypes, parse_dates=['Date'])

    df['Date'] = pd.to_datetime(df['Date'])
    df['Category'].fillna('', inplace=True)
    df['Country'].fillna('', inplace=True)
    df['Category'] = df['Category'].replace('100 Drones for Ukraine', '1000 Drones for Ukraine')
    df['Category'] = df['Category'].replace('Milan', '1000 Drones for Ukraine')


    return df


# Main function
def read_new_PayPal_txs_from_AWS():
    try:
        df_date = pd.read_csv('data/PayPal.csv', usecols=['Date'], parse_dates=['Date'])
        start_date = df_date['Date'].max().strftime('%Y-%m-%d')
    except FileNotFoundError:
        start_date = '2023-03-01'

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    df_new = read_PayPal_txs(start_date)
    etl(df_new)
    return ETL_raw_data()
