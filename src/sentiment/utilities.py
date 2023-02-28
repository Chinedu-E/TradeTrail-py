import requests
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Union
from datetime import datetime

import pandas as pd
import snscrape.modules.twitter as sntwitter


def get_sentiments(string: str):
    ...

#---------------NEWS------------------------#

def search_for_stock_news_links(ticker: str) -> List[str]:
    search_url = 'https://www.google.com/search?q=yahoo+finance+{}&tbm=nws'.format(ticker)
    r = requests.get(search_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    atags = soup.find_all('a')
    hrefs = [link['href'] for link in atags]
    return hrefs


def strip_unwanted_urls(urls: List[str], exclude_list: List[str], limit: int) -> List[str]:
    val = []
    for url in urls:
        if 'https://' in url and not any(exc in url for exc in exclude_list):
            res = re.findall(r'(https?://\S+)', url)[0].split('&')[0]
            val.append(res)
    urls = list(set(val))[:limit]
    return urls


def scrape_and_process(urls: List[str])  -> List[str]:
    articles = []
    for url in urls:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        results = soup.find_all('p')
        text = [res.text for res in results]
        words = ' '.join(text).split(' ')[:350]
        article = ' '.join(words)
        articles.append(article)
    return articles


def summarize(articles: List[str], tokenizer, model) -> List[str]:
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


def get_tweets(ticker: str, limit: int) -> List[List[datetime, str]]:
    tweets_generator = sntwitter.TwitterSearchScraper(f"{ticker} since:2023-02-10 until:2023-02-28").get_items()
        
    tweets = []
    for tweet in tweets_generator:
        if len(tweets) == limit:
            break
        if tweet.lang == "en" and tweets[-1] != tweet.rawContent:
            tweets.append([tweet.date, tweet.rawContent])
    return tweets


def get_tweets_df():
    ...