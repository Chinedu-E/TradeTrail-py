import random
import datetime

import pandas as pd
from decouple import config
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import classification_report, precision_score, recall_score, f1_score, roc_auc_score, accuracy_score

from .. import helpers
from ..clustering import StocksCluster
from . import utilities

import logging

logging.basicConfig(level=logging.INFO)

BUCKET_NAME  = config('S3_BUCKET_NAME')
DATABASE_NAME = "models"
COLLECTION_NAME = "trademodels"

def main():
    data_handler = helpers.TrainingDataHandler()
    cluster = StocksCluster()
    
    cluster_num = cluster.sample()
    stocks = cluster.stocks_from_cluster(cluster=cluster_num)
    training_stock = random.choice(stocks)
    logging.info(f"Training on {training_stock}")
    logging.info(f"Getting data")
    df = data_handler.get_trading_data(training_stock)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True, drop=True)
    df = df.astype(float)
    df.sort_index(ascending=True, inplace=True)
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    
    features = utilities.form_features(df=df)
    features = features[utilities.PREDICTORS]
    labels = utilities.form_labels(df)
    
    splits = [0.8, 0.2]
    X_train, X_test, y_train, y_test = utilities.split_data(features, labels, splits)
    
    
    
    model = RandomForestClassifier(n_estimators=200, min_samples_split=50, random_state=1, n_jobs=-1)
    scaler = MinMaxScaler()
    
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)
    logging.info("Fit model")
    model.fit(X_train, y_train)
    
    
    y_pred = model.predict(X_test)
    today = datetime.datetime.today().strftime('%Y%m%d%H%M%S')
    model_name = f"cluster{cluster_num}_{today}"
    metadata = {}
    metadata["Name"] = model_name
    metadata["Symbol"] = training_stock
    metadata["Cluster"] = cluster_num
    metadata["Rundate"] = today
    metadata["Algorithm"] = model.__class__.__name__
    metadata["Accuracy"] = accuracy_score(y_test, y_pred)
    metadata["F1"] = f1_score(y_test, y_pred)
    metadata["ROC_AUC"] = roc_auc_score(y_test, y_pred)
    metadata["Precision"] = precision_score(y_test, y_pred)
    metadata["Recall"] = recall_score(y_test, y_pred)

    print(classification_report(y_test, y_pred))
    
    
    utilities.save_model(model=model, scaler=scaler, model_name=model_name,
                         bucket_name=BUCKET_NAME, db_name=DATABASE_NAME,
                         collection_name=COLLECTION_NAME, metadata=metadata)
    logging.info("Saved Model")
    
    
    
    

if __name__ == "__main__":
    main()