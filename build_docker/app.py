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
    weekday: float
    source_tag: float
    mat_tag: float
    month: float
    day: float
    hour: float
    minute: float
    week_no: float


@app.post("/predict")
def predict(input_data: PredictionInput):
    # Convertir los datos de entrada a un array NumPy
    input_values = np.array([[input_data.weekday,
                              input_data.source_tag,
                              input_data.mat_tag,
                              input_data.month,
                              input_data.day,
                              input_data.hour,
                              input_data.minute,
                              input_data.week_no]])

    # Realizar la predicción
    prediction = model.predict(input_values)[0]

    # Retornar la predicción
    return {"prediction": prediction.tolist()}

if __name__ == "__main__":
    # Iniciar el servidor FastAPI
    uvicorn.run(app, host="0.0.0.0", port=8000)
