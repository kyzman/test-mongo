import pymongo
import datetime

client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client.sampleDB
collection = db.sample_collection

dt_from = datetime.datetime(2022, 2, 1, 0, 0)
dt_upto = datetime.datetime(2022, 2, 2, 0, 0)
group_type = "hour"

data_range = {"$and": [{"dt": {"$gt": dt_from}}, {"dt": {"$lt": dt_upto}}]}

result = collection.find(data_range).sort("dt", 1)

summary = 0

labels = [dt_from.isoformat()]
dataset = []

for value in result:
    if datetime.datetime.fromisoformat(labels[-1]).__getattribute__(group_type) == value['dt'].__getattribute__(group_type):
        summary += value['value']
    else:
        labels.append(value['dt'].isoformat())
        dataset.append(summary)
        summary = value['value']

print(dataset)
print(labels)
