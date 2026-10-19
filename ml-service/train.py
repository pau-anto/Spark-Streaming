# ======================================================================
# SERVICE ML : entraînement + export ONNX
# Lancement depuis la racine Spark-Streaming/ :  python ml-service/train.py
# ======================================================================

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# 1. Lire les sorties du consumer (features + label)
df = pd.read_parquet("output/train")
X = np.stack(df["features"].values).astype(np.float32)  # une ligne = une image (12 288 valeurs)
y = df["label"].to_numpy(dtype=str) # "lys" ou "tulipes"print(f"{len(df)} images chargées")

# 2. Séparer entraînement (80 %) / validation (20 %)
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# 3. Entraîner le modèle
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 4. Évaluer sur les images de validation
accuracy = accuracy_score(y_val, model.predict(X_val))
print(f"Accuracy validation : {accuracy:.2%}")

# 5. Exporter en ONNX (format lisible par le consumer Scala)
#    zipmap=False : les probabilités sortent en tableau simple
onnx_model = convert_sklearn(
    model,
    initial_types=[("input", FloatTensorType([None, X.shape[1]]))],
    options={id(model): {"zipmap": False}},
    target_opset={"": 17, "ai.onnx.ml": 3},  # version d'ONNX compatible avec le consumer Scala
)

with open("ml-service/model/model.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())

print("Modèle exporté : ml-service/model/model.onnx")