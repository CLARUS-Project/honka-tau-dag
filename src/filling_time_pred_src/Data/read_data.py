import pandas as pd
from io import StringIO
from ids_read.ids_agent_client  import IDSAgentClient
import config
import requests
def read_data() -> pd.DataFrame:

    """
    The function implements the logic to ingest the data and transform it into a pandas format.

    In this code example, a csv file is retrieved from a datasource.
    If not using IDS add your own code to read the datasource
    If using IDS uncomment the example code and replace <<Dataset Provider IP>> with the IP of the dataset provider partipant

    Return:
        A Pandas DataFrame representing the content of the specified file.
    """
    #print("update v1")
    #print("connecting to 194.157.214.66, 1028")

        # ping

    #print("CONNECTION TEST")
    #try:
    #    r1 = requests.get(url="http://194.157.214.74:1028")
    #    print(r1.content)
    #except:
    #    print("cannot connect")
    #print("CONNECTION TEST END")

    try:

        #if not using IDS, your own code
        # ADD YOUR OWN CODE

        IP_addr = "130.230.140.135"
        #IP_addr = "194.157.214.74"
        print("connecting to ", IP_addr)
    
        #if using IDS 
        #Uncomment this block and set the parameters
        #<<Dataset Provider IP>>: IP of the host where the dataset provider connector is deployed
        #<<Dataset Provider Port>>: None if any forwarding of the default port 8086 in the dataset provider connector has been done when deploying otherwise forwarding port
        ids_agent_client = IDSAgentClient()
        # #Start transfer dataset
        #print("connecting to",IP_addr)
        resp= ids_agent_client.get_asset_from_ids(config.MLFLOW_EXPERIMENT,connectorIP=IP_addr, connectorPort="3040")
        if resp == False:
            print("unable to connect")
            return None

        else:
            #Get dataset from agent volume
            print("connected, get data")
            response=ids_agent_client.get_dataset(config.MLFLOW_EXPERIMENT)
            data = StringIO(response)
            df = pd.read_csv(data, delimiter=';', quotechar='"')
            return df

       
    
    except Exception as exc:
        print(f'error:  { str(exc)}') 
        return None
       