from sentiment.news import NewsPipeline
from sentiment.tweets import TwitterPipeline


if __name__ == "__main__":
    news = NewsPipeline()
    twitter = TwitterPipeline()
    news.run()
    twitter.run()