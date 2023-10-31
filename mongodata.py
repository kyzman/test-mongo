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
    return periods


def get_intervals_hour(tstart: datetime, tend: datetime) -> list:
    period_start = tstart
    periods = [period_start.isoformat()]
    interval = timedelta(hours=1)
    while period_start < tend:
        period_end = min(period_start + interval, tend)
        periods.append(period_end.isoformat())
        period_start = period_end
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


    return {"dataset": dataset, "labels": labels}


if __name__ == "__main__":
    dt_from = datetime(2022, 2, 1, 0, 0)
    dt_upto = datetime(2022, 2, 2, 0, 0)
    group_type = "hour"
    my_col = get_mongo_mycollection("mongodb://127.0.0.1:27017")
    print(get_db_data(dt_from, dt_upto, group_type, my_col))

# data_range = {"$and": [{"dt": {"$gt": dt_from}}, {"dt": {"$lt": dt_upto}}]}
#
# result = collection.find(data_range).sort("dt", 1)

# summary = 0
#
# labels = [dt_from.isoformat()]
# dataset = []

# for value in result:
#     if datetime.fromisoformat(labels[-1]).__getattribute__(group_type) == value['dt'].__getattribute__(group_type):
#         summary += value['value']
#     else:
#         labels.append(value['dt'].isoformat())
#         dataset.append(summary)
#         summary = value['value']

