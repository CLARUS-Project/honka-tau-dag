import pandas as pd
import requests
import datetime
import numpy as np
import matplotlib.pyplot as plt
import json
from sklearn.metrics import mean_absolute_error
file_in = pd.read_csv("filtered_set_2024_anom.csv",sep=";")
source_tag = {"sh1":1,
              "sh2":2,
              "sh3":3}
mat_tag = {"Sekatuote siipikarja luokka 3":1,
           "Varpaat, Siipikarja, Luokka 3":2}


def process_data(dataset:pd.DataFrame):
    x_array = []
    y_array = []
    for i_idx,_ in dataset.iterrows():
        if pd.isna(dataset.iloc[i_idx]["Alt filling start"]):
            start_timestamp = datetime.datetime.strptime(dataset.iloc[i_idx]["Filling start"], '%Y-%m-%dT%H:%M:%SZ')
        else:
            start_timestamp = datetime.datetime.strptime(dataset.iloc[i_idx]["Alt filling start"], '%Y-%m-%dT%H:%M:%SZ')
        end_timestamp = datetime.datetime.strptime(dataset.iloc[i_idx]["Filling end"], '%Y-%m-%dT%H:%M:%SZ')
        duration = (end_timestamp-start_timestamp).total_seconds()
        if 500<duration<60000:
            if dataset.iloc[i_idx]["Category"] in mat_tag.keys():
                material_id = mat_tag[dataset.iloc[i_idx]["Category"]]
                sh_id = source_tag[dataset.iloc[i_idx]["SH"]]
                week_no = start_timestamp.isocalendar()[1]
                x_array.append([start_timestamp.weekday(),
                                sh_id,
                                material_id,
                                start_timestamp.month,#not used in 6 features model
                                start_timestamp.day,#not used in 6 features model
                                start_timestamp.hour,
                                start_timestamp.minute,
                                week_no])
                y_array.append(np.log10(duration))
    return x_array, y_array


x_test,y_test = process_data(file_in)
headers = {"Content-Type": "application/json"}
actual_ground_truth_array = []
predicted_array = []
for idx in range(len(x_test)):
    print(x_test[idx])
    print(y_test[idx])
    try:
        data_package2 = {"input_data": x_test[idx],
                         "model": "honka_uc1_sc1", }#change model name

        req2 = requests.get(url="http://localhost:7040/predict_3", params={"data_package": json.dumps(data_package2)},
                            headers=headers)
        # print(req2.content)
        req_result = json.loads(req2.content)["predicted_result"]
        pred_result = float(req_result[0])
        print(pred_result)
        pred_filling_duration = 10**pred_result
        print("filling duration: ", pred_filling_duration)
        predicted_array.append(pred_filling_duration)
        actual_ground_truth_array.append(10**y_test[idx])
    except:
        pass
print("MAE: ", mean_absolute_error(actual_ground_truth_array,predicted_array))