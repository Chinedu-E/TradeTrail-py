import concurrent.futures as cf
import logging
import datetime
from typing import List, Dict, Any


from decouple import config
from yahoo_fin import stock_info
import pandas as pd
import pymongo
from transformers import pipeline
from transformers import PegasusTokenizer, PegasusForConditionalGeneration

from sentiment.utilities import Pipeline, search_for_stock_news_links, strip_unwanted_urls, scrape_and_process, summarize, get_news_df


class NewsPipeline(Pipeline):
    """
    A class for collecting and processing news related to a set of tickers from the S&P 500 index.
    
    Attributes:
    -----------
    _instance : NewsPipeline
        The singleton instance of the NewsPipeline class.
    db_name : str
        The name of the MongoDB database to connect to.
    collection_name : str
        The name of the collection to store the news data.
    summary_model_name : str
        The name of the Pegasus model to use for summarizing news articles.
    sentiment_model_name : str
        The name of the Roberta model to use for sentiment analysis.
    model : transformers.PegasusForConditionalGeneration
        The Pegasus model for summarizing news articles.
    tokenizer : transformers.PegasusTokenizer
        The tokenizer for the Pegasus model.
        
    Methods:
    --------
    __init__():
        Initializes a NewsPipeline instance.
        
    __call__(ticker: str) -> List[Dict[str, Any]]:
        Retrieves news articles related to the specified ticker from the MongoDB collection.
        
    connect_to_database():
        Establishes a connection to the MongoDB database and collection.
        
    run():
        Scrapes and processes news articles related to a set of tickers from the S&P 500 index,
        summarizes the articles, performs sentiment analysis, and stores the data in the MongoDB collection.
        
    __get_ticker_news(ticker: str) -> pandas.DataFrame:
        Helper method for the run method that retrieves, processes, and summarizes news articles
        related to a specific ticker.
    """
    _instance = None
    db_name = "sentiment"
    collection_name = "news"
    summary_model_name = "human-centered-summarization/financial-summarization-pegasus"
    sentiment_model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    model = PegasusForConditionalGeneration.from_pretrained(summary_model_name)
    tokenizer = PegasusTokenizer.from_pretrained(summary_model_name)
    
    
    def __init__(self):
        super().__init__()
        if NewsPipeline._instance:
            self = NewsPipeline._instance
        else:
            NewsPipeline._instance = self
            self.connect_to_database()
    
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
        docs = self.collection.find(query, {"_id": 0})
        docs = [doc for doc in docs]
        return docs
    
    def connect_to_database(self):
        self.client = pymongo.MongoClient(f"mongodb+srv://{config('MONGO_USER')}:{config('MONGO_PASS')}@cluster0.rvb4tg8.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        
    
    def run(self):
        """
        Runs the pipeline for collecting and processing news articles for all stocks in the S&P 500.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        tickers = stock_info.tickers_sp500()
        with cf.ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(NewsPipeline.__get_ticker_news, tickers[:8])
        for result in list(results):
            records = result.to_dict(orient="records")
            try:
                self.collection.insert_many(records)
            except Exception as e:
                print(e)
            
    @staticmethod
    def __get_ticker_news(ticker: str) -> pd.DataFrame:
        """
        Scrape news articles related to a given stock ticker and extract summaries, sentiment scores,
        URLs, and articles using pre-trained models and functions.

        Parameters
        ----------
        ticker : str
            The stock ticker to search news articles for.

        Returns
        -------
        pd.DataFrame
        A pandas DataFrame containing summaries, sentiment scores, URLs, articles, and the date they were added.
        """
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        exclude_list = ['maps', 'policies', 'preferences', 'accounts', 'support']
        
        urls = search_for_stock_news_links(ticker)
        urls = strip_unwanted_urls(urls, exclude_list, limit=3)
        articles = scrape_and_process(urls)
        summaries = summarize(articles, NewsPipeline.tokenizer, NewsPipeline.model)
        
        sentiment = pipeline("sentiment-analysis", model=NewsPipeline.sentiment_model_name)
        scores = sentiment(summaries)
        
        df = get_news_df(ticker, summaries, scores, urls, articles)
        df["date_added"] = [current_date] * len(df)
        
        return df
        
        
if __name__ == "__main__":
    news_pipe = NewsPipeline()
    news_pipe.run()