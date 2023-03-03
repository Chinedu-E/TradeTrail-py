import random
from typing import List, Dict, Any

import concurrent.futures as cf
from yahoo_fin import stock_info
from decouple import config
import pymongo
import pandas as pd
from transformers import pipeline
from utilities import Pipeline, get_tweets


class TwitterPipeline(Pipeline):
    """
    A pipeline to collect tweets related to S&P 500 stocks, and store them along with their sentiment scores 
    in a MongoDB database.

    Attributes:
    -----------
    _instance : TwitterPipeline
        The singleton instance of the TwitterPipeline class.
    db_name : str
        The name of the MongoDB database.
    collection_name : str
        The name of the MongoDB collection.
    sentiment_model_name : str
        The name of the pre-trained sentiment analysis model.

    Methods:
    --------
    __init__():
        Initializes the TwitterPipeline object and connects it to the MongoDB database.
    connect_to_database():
        Connects to the MongoDB database.
    run():
        Runs the pipeline by collecting tweets for 100 randomly selected S&P 500 stocks, and storing them in the 
        MongoDB database.
    __get_tweet_and_sentiment(ticker: str):
        Given a ticker symbol, collects 20 tweets related to the ticker, and calculates the sentiment scores 
        for each tweet.
    __call__(self, ticker: str):
        Retrieves tweets related to the specified ticker symbol from the MongoDB database.
    """
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
        """
        Runs the pipeline by collecting tweets for 100 randomly selected S&P 500 stocks, and storing them in the 
        MongoDB database.
        """
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
    def __get_tweet_and_sentiment(ticker: str):
        """
        Given a ticker symbol, collects 20 tweets related to the ticker, and calculates the sentiment scores 
        for each tweet.

        Parameters:
        -----------
        ticker : str
            The ticker symbol for the stock.

        Returns:
        --------
        df : pandas.DataFrame
            A DataFrame with two columns, "Tweet" and "Sentiment", where "Tweet" is a tweet related to the stock, 
            and "Sentiment" is the sentiment score for the tweet.
        """
        tweets = get_tweets(ticker, limit=20)
        df = pd.DataFrame(tweets, columns=["Date", "Tweet"])
        sentiment = pipeline("sentiment-analysis", model=TwitterPipeline.sentiment_model_name)
        scores = sentiment(list(df["Tweet"].values))
        df["Sentiment"] = [scores[i]["label"] for i in range(len(scores))]
        df["Score"] = [scores[i]["score"] for i in range(len(scores))]
        return df
    
    def __call__(self, ticker: str) ->List[Dict[str, Any]]:
        """Retrieves tweets related to the specified ticker symbol from the MongoDB database.

        Parameters:
        -----------
        ticker : str
            The ticker symbol for the stock.

        Returns:
            List[Dict]: The dictionary has the following fields:
                        - ticker: The ticker symbol of the company.
                        - url: The URL of the full news article.
                        - tweet: The full text of the news article.
                        - summary: The summary text of the news article.
                        - sentiment: The sentiment label associated with the article ('positive', 'negative', or 'neutral').
                        - score: The sentiment score associated with the article.
        """
        query = {"ticker": ticker}
        docs = self.collection.find(query)
        docs = [doc for doc in docs]
        return docs
    
    
    
    
    
    
    
def main():
    twitter_pipe = TwitterPipeline()
    twitter_pipe.run()
    
    
if __name__ == "__main__":
    main()