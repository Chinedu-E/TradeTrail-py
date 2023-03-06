import pandas as pd
import random
from sklearn.cluster import KMeans
from yahoo_fin import stock_info
from typing import List, Literal

from .. import helpers

#from helpers import TrainingDataHandler
from . import utilities


class StocksCluster:
    
    def __init__(self):
        try:
            self.__load_cluster()
        except Exception as e:
            print(e)
            self.n_clusters = 8
            self.run_cluster()
    
    def __call__(self, ticker: str, n: int) -> List[str]:
        cluster = self.cluster_from_stock(ticker)
        stocks = self.stocks_from_cluster(cluster)
        if len(stocks) >= n:
            return stocks[:n]
        extra = self.stocks_from_similar_sector(ticker)
        stocks = stocks + extra
        stocks = list(set(stocks))
        return stocks[:n]
        
    def run_cluster(self):
        df = helpers.TrainingDataHandler.get_clustering_data(column="Adj Close")
        features = utilities.form_features(df)
        kmeans = KMeans(n_clusters=self.n_clusters)
        kmeans.fit(features)
        labels = kmeans.labels_
        df["Cluster"] = labels
        df.to_json("./clustering/cluster.json", orient="records")
        
        
    def get_diverse_portfolio(self, n: int) -> List[str]:
        
        def get_stocks(stocks, limit, curr_quant):
            if curr_quant >= limit:
                return stocks[:limit]
            clusters = random.choices(list(range(self.n_clusters)), k=4)
            for cluster in clusters:
                stock = self.stocks_from_cluster(cluster)[0]
                stocks.append(stock)
            return get_stocks(stocks, n, len(stocks))
        
        stocks = []
        stocks = get_stocks(stocks, n, 0)
        return stocks
        
    def get_similar_portfolio(self, n: int) -> List[str]:
        
        def get_stocks(stocks, limit, curr_quant):
            if curr_quant >= limit:
                return stocks[:limit]
            cluster_num = self.sample()
            stocks = self.stocks_from_cluster(cluster_num)
            random.shuffle(stocks)
            return get_stocks(stocks, n, len(stocks))
        
        stocks = []
        stocks = get_stocks(stocks, n, 0)
        return stocks
    
    def __load_cluster(self):
        self.cluster_df = pd.read_json("./src/clustering/cluster.json", orient="records")
        self.n_clusters = len(self.cluster_df["Cluster"].unique())
        
    def cluster_from_stock(self, ticker: str) -> int:
        if ticker not in self.cluster_df["Symbol"].values:
            return None
        cluster = self.cluster_df[self.cluster_df["Symbol"] == ticker]["Cluster"].values[0]
        return cluster        
        
    def stocks_from_cluster(self, cluster: int) -> List[str]:
        stocks = self.cluster_df[self.cluster_df["Cluster"] == cluster]["Symbol"].values
        stocks = list(stocks)
        random.shuffle(stocks)
        return stocks
        
        
    def sample(self) -> int:
        cluster = random.randrange(0, self.n_clusters)
        return cluster
    
    def stocks_from_similar_sector(self, ticker: str) -> List[str]:
        sector = self.cluster_df[self.cluster_df["Symbol"] == ticker]["GICS Sector"].values[0]
        stocks = self.cluster_df[self.cluster_df["GICS Sector"] == sector]["Symbol"].values
        return list(stocks)
    