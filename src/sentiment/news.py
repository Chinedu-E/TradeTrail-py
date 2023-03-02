import concurrent.futures as cf
import logging
import datetime
from decouple import config

from yahoo_fin import stock_info
import pymongo
from transformers import pipeline
from transformers import PegasusTokenizer, PegasusForConditionalGeneration

from utilities import Pipeline, search_for_stock_news_links, strip_unwanted_urls, scrape_and_process, summarize, get_news_df


class NewsPipeline(Pipeline):
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
    
    def __call__(self):
        ...
    
    def connect_to_database(self):
        self.client = pymongo.MongoClient(f"mongodb+srv://{config['MONGO_USER']}:{config['MONGO_PASS']}@cluster0.rvb4tg8.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        
    
    def run(self):
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
    def __get_ticker_news(ticker: str):
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