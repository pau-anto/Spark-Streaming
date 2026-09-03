# 🎥 Traitement d'images en temps réel — Spark Structured Streaming

Pipeline de traitement d'images en temps réel construit en **Scala** avec **Spark Structured Streaming**, adossé à un **service ML Python** pour l'inférence et visualisé en direct avec **Streamlit**.

> Projet réalisé dans le cadre du cours *Spark Streaming & Structured Streaming* — Cours IABD, 2026

---

## 📐 Architecture

```
../source/..                                                  ../output/..
     │                                                              ▲
     │ ① lecture                                                    │ ⑧ visualisation
     ▼                                                              │
┌─────────────┐   ② écriture batchs    ┌──────────┐            ┌──────────┐
│  Producer   │ ─────────────────────▶ │ ../dest/.. │            │ Streamlit│
│Spark Core/  │                        └──────────┘            │ Data Viz │
│   Scala     │                              │ ③ nouveau fichier └──────────┘
└─────────────┘                              ▼                       ▲
                                        ┌──────────────┐              │ ⑦ chargement modèle
                                        │  Consumer    │──────────────┘
                                        │Struct.       │  ④ écriture résultats
                                        │Streaming     │◀────────────────┐
                                        └──────────────┘                 │
                                              │ ⑤ lecture                │ ⑥ export modèle
                                              ▼                          │
                                        ┌──────────────┐                 │
                                        │ Service ML   │─────────────────┘
                                        │   Python     │
                                        │   Modèle     │
                                        └──────────────┘
```

Le **Producer** simule une source de données réelle en déposant des fichiers dans un répertoire surveillé. Le **Consumer** détecte chaque nouveau fichier, applique la transformation métier (ici : inférence d'un modèle de classification d'images) et écrit les résultats. Le **service ML** entraîne et exporte le modèle utilisé par le Consumer. **Streamlit** lit les résultats en quasi temps réel pour les afficher.

---

## 🧩 Microservices

### 1. Producer (Scala / Spark Core)

Simule une source de données réelle en copiant des fichiers, un batch à la fois, dans le répertoire surveillé (`dest/`).

- Découvre les fichiers source avec `sc.binaryFiles` (fonctionne pour tout type de fichier).
- Copie les fichiers en parallèle via `sc.parallelize(...).foreachPartition`.
- Utilise `FileUtil.copy` de Hadoop — la même couche I/O que Spark lui-même.
- Entièrement basé sur Spark, aucun outil externe nécessaire.
- Paramètres configurables : `batchSize`, `interval`, `recursive` (optionnel), `loop` (optionnel).

### 2. Consumer (Structured Streaming / Scala)

Surveille `dest/`, traite chaque nouveau fichier comme un micro-batch, écrit les résultats dans `output/`.

Pipeline :

1. **Read** — `spark.readStream.format("text").load("dest/")`
2. **Parse** — extrait les champs pertinents de l'entrée brute.
3. **Transform** — applique la logique métier (analyse d'image, extraction de features).
4. **Score / Predict** — charge le modèle exporté et exécute l'inférence.
5. **Write** — `writeStream.format("parquet").start("output/")`

> Le template de départ (comptage de mots) est remplacé aux étapes 2 et 3 par la classification d'images.

### 3. Service ML (Python)

- Entraîne un modèle hors ligne sur un jeu d'images labellisées (Train / Test / Val).
- Exporte le modèle au format **ONNX** ou **Pickle** pour la portabilité.
- Le Consumer charge ce modèle exporté au démarrage pour exécuter l'inférence.

### 4. Visualisation (Streamlit)

- Lit le répertoire `output/` quasiment en temps réel.
- Affiche les prédictions, scores de confiance et métriques de traitement.
- Aucune dépendance à Spark — Python pur.

---

## 📂 Structure du projet

```
.
├── producer/            # Module sbt — Producer Spark Core (Scala)
├── consumer/            # Module sbt — Consumer Structured Streaming (Scala)
├── ml-service/           # Notebook d'entraînement + export du modèle (Python)
│   ├── train.ipynb
│   └── model/            # Modèle exporté (ONNX / Pickle)
├── viz/                  # Application Streamlit
│   └── app.py
├── source/               # Fichiers/images source (entrée du Producer)
├── dest/                 # Répertoire surveillé par le Consumer
├── output/                # Résultats écrits par le Consumer (lus par Streamlit)
├── build.sbt              # Build sbt multi-module
└── README.md
```

---

## ⚙️ Prérequis

- Scala 2.12+ / sbt
- Apache Spark 3.x
- Python 3.9+
- Bibliothèques Python : `streamlit`, `onnxruntime` (ou `scikit-learn`/`torch` selon le modèle), `pandas`

---

## 🚀 Installation & exécution

### 1. Cloner le dépôt

```bash
git clone <url-du-depot>
cd <nom-du-projet>
```

### 2. Compiler les modules Scala

```bash
sbt compile
```

### 3. Entraîner et exporter le modèle ML

```bash
cd ml-service
jupyter notebook train.ipynb
# Le modèle entraîné est exporté dans ml-service/model/
```

### 4. Lancer le Consumer (Structured Streaming)

```bash
sbt "consumer/run"
```

Le Consumer surveille `dest/`, charge le modèle exporté et écrit les résultats dans `output/`.

### 5. Lancer le Producer

```bash
sbt "producer/run --source source/ --dest dest/ --batchSize 5 --interval 5"
```

Le Producer copie les fichiers de `source/` vers `dest/` par batchs, à intervalle régulier.

### 6. Lancer la visualisation

```bash
cd viz
streamlit run app.py
```

Ouvrir le navigateur sur `http://localhost:8501` pour voir les prédictions en temps réel.

---

## 📚 Ressources

- [Guide Structured Streaming (Spark)](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
  
---

## 👤 Auteurs
- AKOUE Eliette
- ANTOGNELLI PAULINE
- MEZOUAR ALia
- SALEMKOUR Tinhinane
