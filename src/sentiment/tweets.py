import random

import concurrent.futures as cf
from yahoo_fin import stock_info
from decouple import config
import pymongo
import pandas as pd
from transformers import pipeline
from utilities import Pipeline, get_tweets


class TwitterPipeline(Pipeline):
    _instance = None
    db_name = "sentiment"
    collection_name = "twitter"
    sentiment_model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    
    def __init__(self):
        super().__init__()
        if TwitterPipeline._instance:
            self = TwitterPipeline._instance
        else:
            TwitterPipeline._instance = self
            self.connect_to_database()
    
    def connect_to_database(self):
        self.client = pymongo.MongoClient(f"mongodb+srv://{config('MONGO_USER')}:{config('MONGO_PASS')}\
                                          @cluster0.rvb4tg8.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
    
    def run(self):
        tickers = stock_info.tickers_sp500()
        tickers = random.choices(tickers, k=100)
        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(TwitterPipeline.__get_tweet_and_sentiment, tickers)
        
        for result in list(results):
            records = result.to_dict(orient="records")
            try:
                self.collection.insert_many(records)
            except Exception as e:
                print(e)
    
    @staticmethod
    def __get_tweet_and_sentiment(ticker):
        tweets = get_tweets(ticker, limit=20)
        df = pd.DataFrame(tweets, columns=["Date", "Tweet"])
        sentiment = pipeline("sentiment-analysis", model=TwitterPipeline.sentiment_model_name)
        scores = sentiment(list(df["Tweet"].values))
        df["Sentiment"] = [scores[i]["label"] for i in range(len(scores))]
        df["Score"] = [scores[i]["score"] for i in range(len(scores))]
        return df
    
    def __call__(self, ticker: str):
        query = {"ticker": ticker}
        docs = self.collection.find(query)
        docs = [doc for doc in docs]
        return docs
    
    
    
    
    
    
    
def main():
    twitter_pipe = TwitterPipeline()
    twitter_pipe.run()
    
    
if __name__ == "__main__":
    main()