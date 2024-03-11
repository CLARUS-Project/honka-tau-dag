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
    df = df[['2303_M1', '2307_M1', '2501_M1', '2501_M4', '2502_M4', '2413_M3', '2650_XT1', '2650_XT2', '2650_XT3', '2650_XT4',"2307_PIC1_PDMEAS", "2406_TIC1_PDMEAS", "2501_PIC1_PDMEAS","2501_SIC1_PDMEAS","2502_PIC1_PDMEAS","2502_SIC1_PDMEAS"]]
    df = df[(df['2650_XT1'] > 0) & (df['2650_XT1'] < 20)]
    df = df[(df['2650_XT2'] > 2) & (df['2650_XT2'] < 10)]
    df = df[df['2650_XT3'] > 60]
    df = df[(df['2650_XT4'] > 0) & (df['2650_XT4'] < 20)]

    df = df[(df['2307_PIC1_PDMEAS'] > 0) & (df['2307_PIC1_PDMEAS'] < 6)]
    df = df[(df['2501_PIC1_PDMEAS'] > 3) & (df['2501_PIC1_PDMEAS'] < 13)]
    df = df[(df['2501_SIC1_PDMEAS'] > 40)]# & (df['2501_SIC1_PDMEAS'] < 130)]
    df = df[(df['2501_PIC1_PDMEAS'] > 3) & (df['2501_PIC1_PDMEAS'] < 10)]
    df = df[(df['2502_SIC1_PDMEAS'] > 30) & (df['2502_SIC1_PDMEAS'] < 140)]
    df = df[(df['2406_TIC1_PDMEAS'] > 70) & (df['2406_TIC1_PDMEAS'] <= 98)]

    x_data = df.drop(['2307_PIC1_PDMEAS', '2501_PIC1_PDMEAS','2502_PIC1_PDMEAS', '2501_SIC1_PDMEAS', '2502_SIC1_PDMEAS', '2406_TIC1_PDMEAS'], axis=1)
    y_data = df['2406_TIC1_PDMEAS'].copy(deep=True)

    return x_data,y_data


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

    return {"dataset1_x_train": dataset1_x_train,
            "dataset1_x_test": dataset1_x_test,
            "dataset1_y_train": dataset1_y_train,
            "dataset1_y_test": dataset1_y_test,
            }
