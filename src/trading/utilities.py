from typing import List, Dict, Any
import tempfile

import numpy as np
import pandas as pd
import yfinance as yf 
from finta import TA
import joblib
import s3fs
import pymongo
from decouple import config


PREDICTORS = ["Open", "High", "Low", "Close", "Volume", "SMA", "RSI", "OBV", "KAMA", "ROC"]

def form_features(ticker: str = None, df: pd.DataFrame = None):
    """
    Extracts relevant features from the given data and returns a DataFrame of the selected features.
    
    Parameters
    ----------
    ticker : str, optional
        The stock symbol to download data for using the yfinance library.
    df : pandas DataFrame, optional
        The data containing the stock data from which to extract features.
        
    Returns
    -------
    pandas DataFrame
        Returns a DataFrame of the selected features.
        
    Raises
    ------
    AssertionError
        If `ticker` is not provided when `df` is None.
    """
    if df is None:
        assert ticker is not None
        df = yf.download(ticker, interval="1m", period="5d")
    df['SMA'] = TA.SMA(df, 30)
    df['RSI'] = TA.RSI(df)
    df['OBV'] = TA.OBV(df)
    df["KAMA"] = TA.KAMA(df)
    df["ROC"] = TA.ROC(df, 30)
    df.fillna(0, inplace=True)
    df= df[PREDICTORS]
    return df


def form_labels(df: pd.DataFrame) -> List[str]:
    """
    Creates a binary target variable based on the "Close" and "Tomorrow" columns of the given DataFrame.
    
    Parameters
    ----------
    df : pandas DataFrame
        The data containing the stock data from which to form the labels.
        
    Returns
    -------
    list
        A list of binary labels for the stock data.
    """
    df["Tomorrow"] = df["Close"].shift(-1)
    df["Target"] = (df["Tomorrow"] > df["Close"]).astype(int)
    return list(df["Target"].values)


def split_data(X, y, splits: List[float]):
    """
    Split the given data into train and test sets based on the provided split ratios.
    
    Parameters
    ----------
    X : pandas DataFrame
        The input data containing the features.
    y : pandas Series
        The target variable to be predicted.
    splits : list of float
        The ratios in which the data should be split into train and test sets.
        The ratios must add up to 1.0.
        
    Returns
    -------
    Tuple of pandas DataFrame
        Returns a tuple containing pandas DataFrame of X splits and y splits respectively.
        
    Raises
    ------
    ValueError
        If the sum of the given split ratios is not equal to 1.0.
    """
    x_splits = []
    y_splits = []
    
    if sum(splits) != 1:
        return ValueError("Bad split, splits must add up to 1.0")
    
    bookmark = 0
    for split in splits:
        to_split = int(len(X)* split)
        nth_x_split = X.iloc[bookmark: bookmark+to_split]
        nth_y_split = y[bookmark: bookmark+to_split]
        x_splits.append(nth_x_split)
        y_splits.append(nth_y_split)
        bookmark += to_split
        
    return *x_splits, *y_splits
    
    
def get_s3():
    return s3fs.S3FileSystem(key=config('AWS_ACCESS_KEY_ID'), secret=config('AWS_SECRET_ACCESS_KEY'))
    
def save_model(model: Any, scaler: Any, model_name: str, bucket_name: str,
               db_name: str, collection_name: str, metadata: Dict[str, Any]) -> None:
    """
    Saves a machine learning model and its scaler to an S3 bucket and metadata about the model to a MongoDB
    collection.

    Parameters
    ----------
    model : Any
        A machine learning model to be saved.
    scaler : Any
        The scaler used to preprocess the data for the machine learning model.
    model_name : str
        The name to give the saved model.
    bucket_name : str
        The name of the S3 bucket to save the model to.
    db_name : str
        The name of the MongoDB database to save metadata to.
    collection_name : str
        The name of the MongoDB collection to save metadata to.
    metadata : Dict[str, Any]
        Metadata about the machine learning model to be saved to MongoDB.

    Returns
    -------
    None
    """
    with tempfile.TemporaryDirectory() as tempdir:
        # Save the model
        joblib.dump(model, f'{tempdir}/{model_name}.joblib')
        joblib.dump(scaler, f'{tempdir}/{model_name}_scaler.joblib')
        
        s3 = get_s3()
        s3.put(f'{tempdir}/{model_name}.joblib', f'{bucket_name}/models/{model_name}.joblib')
        s3.put(f'{tempdir}/{model_name}_scaler.joblib', f'{bucket_name}/models/{model_name}_scaler.joblib')
        
        #Save the metadata in MongoDB
        client = pymongo.MongoClient(f"mongodb+srv://{config('MONGO_USER')}:{config('MONGO_PASS')}@cluster0.rvb4tg8.mongodb.net/?retryWrites=true&w=majority")
        db = client[db_name]
        collection = db[collection_name]
        
        collection.insert_one(metadata)
        
    
    