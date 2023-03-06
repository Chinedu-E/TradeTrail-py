import pandas as pd
import numpy as np



def form_features(df: pd.DataFrame) -> np.ndarray:
    daily_returns = df.pct_change()
    annual_mean_returns = daily_returns.mean()* 252
    annual_return_variance = daily_returns.var()* 252
    
    df2 = pd.DataFrame(df.columns, columns=["Symbol"])
    df2["Returns"] = list(annual_mean_returns)
    df2["Variances"] = list(annual_return_variance)
    df2.fillna(0, inplace=True)
    
    return df2.values