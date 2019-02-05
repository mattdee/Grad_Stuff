import sys
import os
import tweepy
from tweepy import *
from memsql.common import database

# NLP for gathering topics
import spacy

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# https://github.com/cjhutto/vaderSentiment

# Initialize the Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Load English tokenizer, tagger, parser, NER and word vectors
spacy_nlp = spacy.load('en_core_web_sm')



'''
/* Database and table(s) creation */
create database if not exists tweets;
use tweets;


create table if not exists tweets.raw_tweets
(
	tweetid	bigint,
	userscreenname	longtext,
	tweet_creationdate	datetime(6),
	tweet_minute as minute(tweet_creationdate) persisted int,
	tweet_hour as hour(tweet_creationdate) persisted int,
	tweet_dayname as dayname(tweet_creationdate) persisted varchar(20),
	tweet_monthname as monthname(tweet_creationdate) persisted varchar(20),
	tweet_year as year(tweet_creationdate) persisted int,
	user_location	longtext,
	tweet_coordinates	longtext,
	retweet_count	int,
	tweet_text	longtext,
	tweet_compound_score	float,
	tweet_positive_score	float,
	tweet_negative_score	float,
	tweet_neutral_score		float,
	FULLTEXT(tweet_text,userscreenname),
	key(tweet_creationdate,tweet_text,userscreenname) using clustered columnstore,
	shard key (tweetid)
);
'''
# Connect to database
host = '127.0.0.1'
db = 'tweets'
user = 'root'
pwd =''
port = '3306'
try:
	conn = database.connect(host=host,user=user,database=db)
except Exception:
	print("Database connection failed")
	sys.exit(1)


# Set Twitter access creds
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET =  ""
CONSUMER_KEY =  ""
CONSUMER_SECRET =  ""

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)


# Create a Twitter API listener
class StreamListener(tweepy.StreamListener):

	def on_status(self, status):
		print(status.text)
		# if status.retweeted:
		# 	return

		id = status.id_str
		userscreenname = status.user.screen_name
		tweetcdate = str(status.created_at)
		userlocation = str(status.user.location)
		tweetcoord = str(status.coordinates)
		retweetcnt = str(status.retweet_count)
		text = status.text

		sentimentScore = analyzer.polarity_scores(text)

		compoundScore = str(sentimentScore["compound"])
		postiveScore = str(sentimentScore["pos"])
		negativeScore = str(sentimentScore["neg"])
		neutralScore = str(sentimentScore["neu"])

		print(compoundScore,postiveScore,negativeScore,neutralScore)

		# Find named entities, phrases and concepts
		docContext = spacy_nlp(text)
		for entity in docContext.ents:
			entity_text = entity.text
			entity_label = entity.label_
			entity_id = id
			print (entity_id,','+entity_text,','+entity_label)
			try:
				conn = database.connect(host=host,user=user,database=db)
				# Insert entity data into the entities table
				insert_entities = '''insert into tweets.entities(tweetid,entity_text,entity_label) values(%s,'%s','%s');''' % (entity_id, entity_text, entity_label)

				insert2 = conn.execute(insert_entities)
				print(insert_entities)
			except Exception:
				print("Could not insert Entities.")



		# Insert raw tweet data into raw_tweets table
		insert_tweets = '''insert into tweets.raw_tweets(tweetid,userscreenname,tweet_creationdate,user_location,tweet_coordinates,retweet_count,tweet_text,tweet_compound_score,tweet_positive_score,tweet_negative_score,tweet_neutral_score) values(%s,'%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s');''' % (id,userscreenname,tweetcdate,userlocation,tweetcoord,retweetcnt,text,compoundScore,postiveScore,negativeScore,neutralScore)




		try:
			conn = database.connect(host=host,user=user,database=db)
			insert1 = conn.execute(insert_tweets)

			print(insert_tweets)

		except Exception:
			print("Could not insert Tweet.")
			pass


	def on_error(self, status_code):
		if status_code == 420:
			return False



if __name__ == '__main__':
	try:
		# Create an instance of the listener
		stream_listener = StreamListener()
		stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
		stream.filter(track=["twitter"],languages=["en"])
	except KeyboardInterrupt:
		print("User cancel")
		sys.exit(1)


