import pickle
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
import uvicorn
import json

# Definir el modelo FastAPI
app = FastAPI()

# Cargar el modelo desde un archivo local
model_path = "model/model.pkl"  # Reemplaza con la ruta a tu modelo
with open(model_path, "rb") as model_file:
    model = pickle.load(model_file)

# Definir el esquema de entrada para las predicciones
class PredictionInput(BaseModel):
    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float
    total_sulfur_dioxide: float
    density: float
    pH: float
    sulphates: float
    alcohol: float

@app.post("/predict")
def predict(data_package):
    data_decoded = json.loads(data_package)
    modelId = data_decoded["model"]
    datos = data_decoded["input_data"]
    logger.info('Predicción del modelo modelId: ' + str(modelId))
    logger.info('Predicción del modelo con datos: ' + str(datos))
    # print(os.listdir())
    # decoy_num = np.array([1])
    # return {"predicted_result":decoy_num.tolist()}
    # Carga el modelo utilizando pickle u otra biblioteca adecuada
    modelpath = './model/' + modelId + '/model.pkl'
    logger.info('Load model from: ' + modelpath)
    with open(modelpath, 'rb') as archivo:
        model = pickle.load(archivo)

    if (model is None):
        return ('No model found')
    inp_data = np.array(datos).reshape(-1, len(datos))
    # trans_data = np.array([*eval(datos).values()]).reshape(1,-1)
    pred_model = model.predict(inp_data)

    # logger.info('Predicción del modelo: '+ pred_model)

    return {"predicted_result": pred_model.tolist()}

if __name__ == "__main__":
    # Iniciar el servidor FastAPI
    uvicorn.run(app, host="0.0.0.0", port=8000)
