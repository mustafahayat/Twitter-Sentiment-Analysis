from pymongo import MongoClient
import pandas as pd
import re
from kafka import KafkaConsumer
from textblob import TextBlob
import json
# from wordcloud import WordCloud

try:
    # Create connection with MongoDb
    client = MongoClient("localhost", 27017)
    # Create or access the Database
    db = client.AfghanInfo
    print("Connection Succeed")
except:
    print("Connection failed")

consumer = KafkaConsumer('afghanistan', group_id='afghan-peace-tweets', auto_offset_reset='earliest',
                         bootstrap_servers=['localhost:9092'])
# consumer = KafkaConsumer("afghanistan", group_id="afghanistan-group-01", auto_offset_reset='earliest',
#                          bootstrap_servers=['localhost:9092'])


# =======================================================================================================================
# Function for clearing the tweets from unnecessary tags and links
def clean_tweets(text):
    # Before cleaning
    # print("--------------------------------------------------------------------------")
    # print("Uncleaned: " + text)

    text = re.sub('@[A-Za-z0-9]', '', text)  # removing the @mention person
    text = re.sub('#', '', text)  # remove the hash tag
    text = re.sub('https?:\/\/\S+', '', text)  # Removing hyperlink

    # After cleaning
    # print("Cleaned: " + text)
    # print("--------------------------------------------------------------------------")
    # print("The clean text is: " + text)
    return text


# ======================================================================================================================

#  Function to get the subjectivity from the text
def get_subjectivity(text):

    # language detection and use of appropriate sentiment analysis module
    # blob = TextBlob(text)
    # if blob.detect_language == 'en':
    #     return TextBlob(text).sentiment.subjectivity
    # else:
    # The translation will work online (I think)
    #     blob.translate(to='en')
    #     return TextBlob(text).sentiment.subjectivity

    return TextBlob(text).sentiment.subjectivity


# Function to get the polarity from the text
def get_polarity(text):
    return TextBlob(text).sentiment.polarity


# Create a function to compute negative (-1), neutral (0) and positive (+1) analysis
def get_anlaysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'


# Consume the data form the Collection named tweets_info in MongoDb
j = 0;
data_list = {}
negative = 0
positive = 0
neutral = 0

# df = pd.DataFrame([tweet['text'] for tweet in consumer], columns=['Tweets'])
# df.head()

for msg in consumer:
    # Create a data from
    # print(msg)

    json_data = json.loads(msg.value.decode('utf-8'))
    record = json.loads(json_data)

    # make checks for retweet and extended tweet-->done for truncated text
    if "retweeted_status" in record:
        try:
            text = record['retweeted_status']['extended_tweet']['full_text']
        except:
            text = record['retweeted_status']['text']
    else:
        try:
            text = record['extended_tweet']['full_text']
        except:
            text = record['text']

    df = pd.DataFrame([text], columns=['Tweets'])
    # print(record)
    # print(record['text'])

    # Now let's clean the data from unnecessary tags like @ symbol, hyperlinks, RTs
    df['Tweets'] = df['Tweets'].apply(clean_tweets)
    df['Subjectivity'] = df['Tweets'].apply(get_subjectivity)
    df['Polarity'] = df['Tweets'].apply(get_polarity)
    df['Analysis'] = df['Polarity'].apply(get_anlaysis)

    text = df['Tweets'][0]
    subjectivity = df['Subjectivity'][0]
    polarity = df['Polarity'][0]
    analysis = df['Analysis'][0]

    # create dictionary and ingest data into mongo
    try:
        tweets_record = {
            'username': record['user']['name'],
            'creation_datetime': record['created_at'],
            'location': record['user']['location'],
            'user_description': record['user']['description'],
            'followers': record['user']['followers_count'],
            'retweets': record['retweet_count'],
            'favorites': record['favorite_count'],
            "text": text,
            "subjectivity": subjectivity,
            "polarity": polarity,
            "analysis": analysis
        }

        rec_id = db.tweets_info.insert_one(tweets_record)
        print("Data inserted with record ids", rec_id)

    except:
        print("Could not insertInMongo")



    # print(df)
    # Printing positive tweets
    # print('Printing positive tweets:\n')
    # sortedDF = df.sort_values(by=['Polarity'])  # Sort the tweets

    # for i in range(0, sortedDF.shape[0]):  # Her the shape is the use for getting the numbe of rows and columns
    #     # The shape[0] mean that we only git the row of the DataFrame
    #
    #     if (sortedDF['Analysis'][i] == 'Positive'):
    #         # print(str(j) + ') ' + sortedDF['Tweets'][i])
    #         print()
    #         j = j + 1
    #         positive += df['Analysis'].value_counts()

    # Printing positive tweets
    # print('Printing Neutral tweets:\n')
    # sortedDF = df.sort_values(by=['Polarity'])  # Sort the tweets

    # for i in range(0, sortedDF.shape[0]):  # Her the shape is the use for getting the numbe of rows and columns
    #     # The shape[0] mean that we only git the row of the DataFrame
    #
    #     if (sortedDF['Analysis'][i] == 'Neutral'):
    #         # print(str(j) + ') ' + sortedDF['Tweets'][i])
    #         print()
    #         j = j + 1
    #         neutral += df['Analysis'].value_counts()

    # Printing negative tweets
    # print('Printing negative tweets:\n')
    # j = 1
    # sortedDF = df.sort_values(by=['Polarity'], ascending=False)  # Sort the tweets
    # for i in range(0, sortedDF.shape[0]):
    #     if (sortedDF['Analysis'][i] == 'Negative'):
    #         # print(str(j) + ') '+sortedDF['Tweets'][i])
    #         print()
    #         j = j + 1
    #         # Count the negative tweets
    #         negative += df['Analysis'].value_counts()

    # ================================================ Visualization of the data ===============================================
    # Plotting
    # plt.figure(figsize=(8, 6))
    # for i in range(0, df.shape[0]):
    #     plt.scatter(df["Polarity"][i], df["Subjectivity"][i], color='Blue')
    #     # plt.scatter(x,y,color)
    # plt.title('Sentiment Analysis')
    # plt.xlabel('Polarity')
    # plt.ylabel('Subjectivity')
    # plt.show()

    # Plotting and visualizing the counts
    # plt.title('Sentiment Analysis')
    # plt.xlabel('Sentiment')
    # plt.ylabel('Counts')
    # # df['Analysis'].value_counts().plot(kind='bar')
    # positive.plot(kind='bar', color='green')
    # negative.plot(kind='bar', color='red')
    # neutral.plot(kind='bar', color='yellow')
    # plt.show()

    # ============================================ End fo visualization ========================================================

# print("Positive Tweets: " + str(positive))
# print("Neutral Tweets: " + str(neutral))
# print("Negative Tweets: " + str(negative))
