import pandas as pd
import datetime as dt

def sum_by_period(data, period, value = 'Amount'):
    return pd.DataFrame(data[value].groupby(data['Date'].dt.to_period(period)).sum())

def sum_category_by_date(category_name, period, data, category, value = 'Amount'):
    return pd.DataFrame(data[((
            data[category] == category_name))][value].groupby(
            data['Date'].dt.to_period(period)).sum().reset_index(name = category_name))


def sum_by_period_by_category(categories, period, data, category, value = 'Amount'):
    data_frames = []
    for category_name in categories:
        data_frames.append(sum_category_by_date(category_name, period, data, category, value))

    from functools import reduce
    return reduce(lambda left, right: pd.merge(left, right, on='Date', how='outer'), data_frames)


def count_category_by_date(category_name, period, data, category):
    return pd.DataFrame(data[((
            data[category] == category_name))]['Amount'].groupby(
            data['Date'].dt.to_period(period)).count().reset_index(name = category_name))
