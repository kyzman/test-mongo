import pymongo
from datetime import datetime, timedelta


def get_mongo_mycollection(url):
    client = pymongo.MongoClient(url)
    db = client.sampleDB
    collection = db.sample_collection
    return collection


def get_intervals_day(tstart: datetime, tend: datetime) -> list:
    period_start = tstart
    periods = []
    interval = timedelta(days=1)
    while period_start < tend:
        periods.append(period_start.isoformat())
        period_end = min(period_start + interval, tend)
        period_start = period_end
    if not tend.day == datetime.fromisoformat(periods[-1]).day:
        periods.append(tend.isoformat())
    return periods


def get_intervals_hour(tstart: datetime, tend: datetime) -> list:
    period_start = tstart
    periods = []
    interval = timedelta(hours=1)
    while period_start < tend:
        periods.append(period_start.isoformat())
        period_end = min(period_start + interval, tend)
        period_start = period_end
    if not tend.hour == datetime.fromisoformat(periods[-1]).hour:
        periods.append(tend.isoformat())
    return periods


def get_intervals_month(start: datetime, end: datetime) -> list:
    cursor = start
    months = [cursor.isoformat()]
    while cursor <= end:
        if cursor.month == datetime.fromisoformat(months[-1]).month:
            cursor += timedelta(weeks=1)
        else:
            months.append(datetime(cursor.year, cursor.month, 1, 0, 0).isoformat())

    return months


def get_db_data(start, end, interval, collection) -> dict:
    labels = ["Некорректный диапазон!"]

    if interval == 'day':
        labels = get_intervals_day(start, end)
    elif interval == 'hour':
        labels = get_intervals_hour(start, end)
    elif interval == 'month':
        labels = get_intervals_month(start, end)

    dataset = []
    pos = 0
    while pos < len(labels) - 1:
        data_range = {"$and": [{"dt": {"$gte": datetime.fromisoformat(labels[pos])}},
                               {"dt": {"$lt": datetime.fromisoformat(labels[pos + 1])}}]}
        result = collection.find(data_range).sort("dt", 1)
        summary = 0
        for value in result:
            summary += value['value']
        dataset.append(summary)
        pos += 1

    data_range = {"$and": [{"dt": {"$gte": datetime.fromisoformat(labels[-1])}},
                               {"dt": {"$lte": end}}]}
    result = collection.find(data_range).sort("dt", 1)
    summary = 0
    for value in result:
        summary += value['value']
    dataset.append(summary)

    return {"dataset": dataset, "labels": labels}

