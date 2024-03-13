"""
This module provides the read_data function, which is utilized by the pipeline orchestrator (Airflow) for data ingestion. 
The function implements the logic to ingest the data and transform it into a pandas format. If any additional auxiliary 
functions are required to accomplish this step, they can be defined within the same script or separated into different 
scripts and included in the Data directory.
"""

import pandas as pd
from IDS_templates.rest_ids_consumer_connector import RestIDSConsumerConnector
import config

def read_data() -> pd.DataFrame:
    """
    The function implements the logic to ingest the data and transform it into a pandas format.

    In this code example, a csv file is retrieved from a url.

    Return:
        A Pandas DataFrame representing the content of the specified file.
    """
    import os
    cwd = os.getcwd()
    print(cwd)
    try:
        ids_consumer = RestIDSConsumerConnector()
        data = ids_consumer.get_external_artifact_by_resource_title(
            config.MLFLOW_EXPERIMENT,
            config.TRUE_CONNECTOR_EDGE_IP,
            config.TRUE_CONNECTOR_EDGE_PORT,
            config.TRUE_CONNECTOR_CLOUD_IP,
            config.TRUE_CONNECTOR_CLOUD_PORT
        )

        df = pd.read_csv(data, delimiter=';', quotechar='"')
    except:
        df = pd.read_csv("logistic_dataset_filling_time_2021_2023.csv", delimiter=';', quotechar='"')
    return df