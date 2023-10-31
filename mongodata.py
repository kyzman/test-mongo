import pymongo
from datetime import datetime, timedelta


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

def get_db_data(start, end, interval) -> dict:
    ...



client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client.sampleDB
collection = db.sample_collection

dt_from = datetime(2022, 2, 1, 0, 0)
dt_upto = datetime(2022, 2, 2, 0, 0)
group_type = "hour"

labels = "Некорректный диапазон!"

if group_type == 'day':
    labels = get_intervals_day(dt_from, dt_upto)
elif group_type == 'hour':
    labels = get_intervals_hour(dt_from, dt_upto)
elif group_type == 'month':
    labels = get_intervals_month(dt_from, dt_upto)

dataset = []
pos = 0
while pos < len(labels)-1:
    data_range = {"$and": [{"dt": {"$gte": datetime.fromisoformat(labels[pos])}}, {"dt": {"$lt": datetime.fromisoformat(labels[pos+1])}}]}
    result = collection.find(data_range).sort("dt", 1)
    summary = 0
    for value in result:
        summary += value['value']
    dataset.append(summary)
    pos += 1



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

print(dataset)
print(len(labels))
print(labels)
