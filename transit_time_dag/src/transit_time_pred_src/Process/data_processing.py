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
def process_core(necessary_data_columns_frame):
    source_tag = {"sh1": "1",
                  "sh2": "2",
                  "sh3": "3"}
    truck_dict = {'truck1': '1', 'truck2': '2', 'truck3': '3', 'truck4': '4', 'truck5': '5',
                  'truck6': '6', 'truck7': '7', 'truck8': '8', 'truck9': '9', 'truck10': '10'}
    necessary_data_columns_frame['SH'] = necessary_data_columns_frame['SH'].replace(source_tag).astype(np.uint8)
    necessary_data_columns_frame['Plate'] = necessary_data_columns_frame['Plate'].replace(truck_dict).astype(np.uint8)
    necessary_data_columns_frame.drop(['Weight', 'Category'], axis=1, inplace=True)
    necessary_data_columns_frame.drop(['Filling start', 'Alt filling start', 'Filling end', 'Loading', 'Unload'], axis=1, inplace=True)
    for c in ['Leaving', 'Arrival']:
        try:
            necessary_data_columns_frame[c] = necessary_data_columns_frame[c].apply(datetime.fromisoformat)
        except:
            necessary_data_columns_frame[c] = necessary_data_columns_frame.to_datetime(necessary_data_columns_frame[c], format='%Y-%m-%dT%H:%M:%SZ')
    y = necessary_data_columns_frame['Arrival'] - necessary_data_columns_frame['Leaving']
    y = y.apply(lambda x: x.total_seconds())
    indx = y.index[(y > 3000) & (y < 13000)]
    necessary_data_columns_frame = necessary_data_columns_frame.loc[indx]
    y = y[indx]
    c = 'Leaving'
    necessary_data_columns_frame[c + ' Week No.'] = necessary_data_columns_frame[c].apply(lambda x: x.weekofyear)
    necessary_data_columns_frame[c + ' Day'] = necessary_data_columns_frame[c].apply(lambda x: x.weekday())
    necessary_data_columns_frame[c + ' Hour'] = necessary_data_columns_frame[c].apply(lambda x: x.hour)
    necessary_data_columns_frame[c + ' Minute'] = necessary_data_columns_frame[c].apply(lambda x: x.minute)
    necessary_data_columns_frame.drop(c, axis=1, inplace=True)
    necessary_data_columns_frame.drop(['Arrival', 'trip_id'], axis=1, inplace=True)
    necessary_data_columns_frame.head()
    x_data = necessary_data_columns_frame.copy()
    return x_data,y

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
            "dataset1_y_train": dataset1_y_train,
            "dataset1_x_test": dataset1_x_test,
            "dataset1_y_test": dataset1_y_test,}
