import requests
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Union, Any
from abc import ABC, abstractmethod

import pandas as pd
import snscrape.modules.twitter as sntwitter



#---------------NEWS------------------------#

def search_for_stock_news_links(ticker: str) -> List[str]:
    """
    Search for news links related to a specific stock ticker on Yahoo Finance.

    Parameters:
    ticker (str): The stock ticker to search for news links.

    Returns:
    List[str]: A list of URLs linking to news articles related to the specified stock ticker.
    """
    search_url = 'https://www.google.com/search?q=yahoo+finance+{}&tbm=nws'.format(ticker)
    r = requests.get(search_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    atags = soup.find_all('a')
    hrefs = [link['href'] for link in atags]
    return hrefs


def strip_unwanted_urls(urls: List[str], exclude_list: List[str], limit: int) -> List[str]:
    """
    Return a list of cleaned URLs from a list of URLs by stripping unwanted URLs.

    Args:
        urls (List[str]): A list of URLs to be cleaned.
        exclude_list (List[str]): A list of strings representing keywords to be excluded.
        limit (int): An integer representing the maximum number of cleaned URLs to return.

    Returns:
        List[str]: A list of cleaned URLs.

    Raises:
        None
    """
    val = []
    for url in urls:
        if 'https://' in url and not any(exc in url for exc in exclude_list):
            res = re.findall(r'(https?://\S+)', url)[0].split('&')[0]
            val.append(res)
    urls = list(set(val))[:limit]
    return urls


def scrape_and_process(urls: List[str])  -> List[str]:
    """
    Scrape the text from the first 350 words of each URL in a list of URLs and return them as a list of strings.

    Parameters
    ----------
    urls : list of str
        A list of URLs to scrape and process.

    Returns
    -------
    list of str
        A list of strings, where each string is the text of the first 350 words of a URL.

    Raises
    ------
    requests.exceptions.RequestException
        If there is an error retrieving a URL.
    """
    articles = []
    for url in urls:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        results = soup.find_all('p')
        text = [res.text for res in results]
        words = ' '.join(text).split(' ')[:320]
        article = ' '.join(words)
        articles.append(article)
    return articles


def summarize(articles: List[str], tokenizer, model) -> List[str]:
    """
    Summarizes a list of articles using a pre-trained transformer model and tokenizer.

    Args:
        articles (List[str]): A list of strings representing the articles to be summarized.
        tokenizer: The tokenizer object used to tokenize the input articles.
        model: The transformer model used to generate the summaries.

    Returns:
        List[str]: A list of strings representing the summaries of the input articles.

    Raises:
        None
    """
    summaries = []
    for article in articles:
        input_ids = tokenizer.encode(article, return_tensors="pt")
        output = model.generate(input_ids, max_length=100, num_beams=5, early_stopping=True)
        summary = tokenizer.decode(output[0], skip_special_tokens=True)
        summaries.append(summary)
    return summaries


def get_news_df(ticker: str, summaries: List[str],
                scores: Dict[str, Union[str, float]],
                urls: List[str], articles: List[str]) -> pd.DataFrame:
    """
    Create a Pandas DataFrame containing news data.

    Parameters
    ----------
    ticker : str
        The ticker symbol of the company associated with the news articles.
    summaries : list of str
        A list of article summary texts.
    scores : dict
        A dictionary containing sentiment scores for each article.
        The keys are strings containing the article labels ('positive', 'negative', or 'neutral'),
        and the values are floats representing the corresponding sentiment scores.
    urls : list of str
        A list of URLs linking to the full news articles.
    articles : list of str
        A list of full news article texts.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the news data.
        The DataFrame has the following columns:
            - ticker: The ticker symbol of the company.
            - url: The URL of the full news article.
            - text: The full text of the news article.
            - summary: The summary text of the news article.
            - sentiment: The sentiment label associated with the article ('positive', 'negative', or 'neutral').
            - score: The sentiment score associated with the article.
    """
    output = []
    for i in range(len(summaries)):
        row = [
            ticker,
            urls[i],
            articles[i],
            summaries[i],
            scores[i]['label'],
            scores[i]['score'],
        ]
        output.append(row)
    df = pd.DataFrame(output, columns=['ticker','url', 'text', 'summary', 'sentiment', 'score'])
    return df

#-------------TWEETS--------------------#


def get_tweets(ticker: str, limit: int) -> List[List]:
    """
    Get a list of English tweets related to a stock ticker within a specific time period.
    
    Parameters:
    ticker (str): A stock ticker symbol to search for tweets.
    limit (int): Maximum number of tweets to return.
    
    Returns:
    List[List[datetime, str]]: A list of tweets containing the datetime of each tweet and its content as a string.
    
    Example:
    >>> get_tweets("AAPL", 100)
    [[datetime.datetime(2023, 2, 27, 23, 59, 59), "Just bought some shares of AAPL. Excited to see where it goes!"],
     [datetime.datetime(2023, 2, 26, 12, 34, 56), "AAPL just announced a new product lineup. Can't wait to see what's in store!"],
     [datetime.datetime(2023, 2, 24, 9, 0, 1), "Sold all my shares of AAPL today. Time to move on to something else."],
     ...
    ]
    """
    tweets_generator = sntwitter.TwitterSearchScraper(f"{ticker} since:2023-02-10 until:2023-02-28").get_items()
        
    tweets = []
    for tweet in tweets_generator:
        if len(tweets) == limit:
            break
        if tweet.lang == "en":
            if len(tweets) > 0 and tweets[-1] != tweet.rawContent:
                tweets.append([tweet.date, tweet.rawContent])
            else:
                tweets.append([tweet.date, tweet.rawContent])
    return tweets

    
#----------------------------------#
    
class Pipeline(ABC):
    """
    Abstract base class for defining data pipelines.
    
    Parameters
    ----------
    None
    
    Attributes
    ----------
    None
    
    Methods
    -------
    __call__(*args, **kwds)
        Abstract method to define the pipeline logic.
        
    run()
        Abstract method to run the pipeline.
        
    connect_to_database()
        Abstract method to connect to a database
        
    Raises
    ------
    NotImplementedError
        If any of the abstract methods are not implemented in a subclass.
        
    Returns
    -------
    Any
        Depending on the implementation of the `__call__` method.
    """
    @abstractmethod
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        ...
    
    @abstractmethod
    def run(self):
        ...
        
    @abstractmethod
    def connect_to_database(self):
        ...    
