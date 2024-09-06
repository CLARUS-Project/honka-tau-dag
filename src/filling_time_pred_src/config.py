import os

# MLFLOW MODEL TRACKING
MLFLOW_ENDPOINT = os.getenv("MLFLOW_ENDPOINT")
MLFLOW_EXPERIMENT = "CLARUS_HK_UC1_SC1_filling_time_v2"
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

# METRIC FOR BEST MODEL
METRIC_BM = "mae"
METRIC_BM_TYPE = "min"

# IDS
TRUE_CONNECTOR_EDGE_IP = "130.230.140.135"#os.getenv("TRUE_CONNECTOR_EDGE_IP") # Clarus edge services
TRUE_CONNECTOR_EDGE_PORT = 3041#os.getenv("TRUE_CONNECTOR_EDGE_PORT") # "8889"
TRUE_CONNECTOR_CLOUD_IP = os.getenv("TRUE_CONNECTOR_CLOUD_IP") # Training connector
TRUE_CONNECTOR_CLOUD_PORT = os.getenv("TRUE_CONNECTOR_CLOUD_PORT") # "8184"
