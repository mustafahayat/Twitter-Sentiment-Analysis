# Import necessary modules
import json
import pykafka
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Import the twitter credentials
import TwitterCredentials as tc


# Create class for tweets listener and extend the class from the Stream Listener class
class TweetsListener(StreamListener):

    def __init__(self):
        # Connect with the local server of the kafka
        self.client = pykafka.KafkaClient("localhost:9092")
        # Produce the data to the following topic
        self.producer = self.client.topics[bytes('afghanistan', 'ascii')].get_producer()

    #     mean if we receive the data
    def on_data(self, raw_data):
        # push data to producer

        self.producer.produce(bytes(json.dumps(raw_data), 'ascii'))
        return True

    def on_error(self, status_code):
        print(status_code)
        return False;


if __name__ == '__main__':
    print("Hello, World")

    # if the tweets are contain these words, these tweets will be fetched.

    words = ["ashraf ghani", "afghanistan", "kabul explosion", "abdullah abdullah", "taliban",
             "zalmai khalilzad", "taliban", "qatar", "pease", "war", "afghan", "afghan army", "bomb blasting",
             "crona virus", "covid19", "covid-19", "mula brother"]

    # Twitter api credentials

    auth = OAuthHandler(tc.CONSUMER_KEY, tc.CONSUMER_SECRET)
    auth.set_access_token(tc.ACCESS_TOKEN, tc.ACCESS_TOKEN_SECRET)

    twitter_stream = Stream(auth, TweetsListener(), tweet_mode='extended')
    twitter_stream.filter(words)
