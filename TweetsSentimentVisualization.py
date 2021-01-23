# import necessary modules

import matplotlib.pyplot as plt

plt.style.use('fivethirtyeight')
from pymongo import MongoClient

try:
    client = MongoClient("localhost", 27017);
    db = client.AfghanInfo
    print("Connection succeed")
except:
    print("Connection failed")

positive = 0
negative = 0
neutral = 0
for record in db.tweets_info.find().limit(100):
    if (record['analysis'] == 'Neutral'):
        neutral += 1
    elif record['analysis'] == 'Positive':
        positive += 1
    else:
        negative += 1

print(positive, negative, neutral)

import matplotlib.pyplot as plt

plt.bar(["positive", "neutral", "negative"], [positive, neutral, negative])
plt.suptitle("Sentiment Analysis")
plt.show()
