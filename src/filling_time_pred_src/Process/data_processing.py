"""
This module defines the data_processing function used by the pipeline orchestrator to perform data preprocessing. 
This function defines the logic for data preprocessing. Any adidtional function needed to perform this step can 
be defined within this script itself or split into different scripts and included in the Process directory.
"""

from sklearn.model_selection import train_test_split
from typing import Dict, Any
import pandas as pd
import datetime
import numpy as np
def process_core(df):
    #df["Filling start"] = pd.to_datetime(df["Filling start"], format='%d.%m.%Y %H:%M')
    #df['Day'] = df["Alt filling start"].apply(lambda x: x.day).astype(np.float32)
    #df['Month'] = df["Alt filling start"].apply(lambda x: x.month).astype(np.float32)
    #df['Weekday'] = df["Alt filling start"].apply(lambda x: x.weekday()).astype(np.float32)
    #df['Hour'] = df["Alt filling start"].apply(lambda x: x.hour).astype(np.float32)
    #df['Minute'] = df["Alt filling start"].apply(lambda x: x.minute).astype(np.float32)
    df = df[(df["filling duration"] > 3600) & (df["filling duration"] < 60000)]
    df.reset_index(drop=True,inplace=True)
    sorted_x_array_processed_data = df.drop(columns="filling duration")
    sorted_y_array_processed_data = df["filling duration"]
    return sorted_x_array_processed_data,sorted_y_array_processed_data

def data_processing(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform data preprocessing on the input dataframe and transform it into train and validation datasets.

    In this code example, the input dataframe is divided into train_x, train_y, val_x and val_y dataframes.

    Args:
        df: The input dataframe containing the data to be preprocessed.

    Return:
        A dictionary containing the preprocessed data.
    """
    # ADD YOUR CODE HERE
    dataset1_x, dataset1_y = process_core(df)
    dataset1_x_train, dataset1_x_test, dataset1_y_train, dataset1_y_test = train_test_split(dataset1_x,dataset1_y,test_size=0.3, random_state= 1)
    #print(dataset1_x)
    #print(len(dataset1_x))
    dataset1_y_train = dataset1_y_train.apply(lambda x: np.log10(x))

    return {"dataset1_x_train": dataset1_x_train,
            "dataset1_y_train": dataset1_y_train,
            "dataset1_x_test": dataset1_x_test,
            "dataset1_y_test": dataset1_y_test,}
