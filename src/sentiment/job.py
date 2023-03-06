from .news import NewsPipeline
from .tweets import TwitterPipeline


if __name__ == "__main__":
    news = NewsPipeline()
    twitter = TwitterPipeline()
    news.run()
    twitter.run()