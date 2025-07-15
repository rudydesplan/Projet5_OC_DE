
# 🏥 Healthcare CSV → MongoDB Loader

Ce projet fournit un pipeline **robuste, testé, dockerisé et extensible** pour charger des données patients depuis un fichier CSV vers une base MongoDB.

---

## 🚀 Fonctionnalités principales

- 🧩 **Schéma BSON JSON validé** automatiquement
- 📦 **Index MongoDB optimisés**
- 📂 **Insertion batch via `bulk_write` (rapide et résiliente)**
- 📆 **Conversion temporelle timezone-aware** avec `pendulum`
- 🧪 **Tests unitaires isolés avec `pytest + mongomock`**
- 📊 **Contrôles d'intégrité avant / après migration**
- 🐳 **Conteneurisation complète via Docker & Docker Compose**
- 🛠 **Automatisation via `Makefile`**

---

## 🗂 Arborescence du projet

```bash
project-root/
├── Makefile                    ← Commandes rapides
├── docker-compose.yml          ← Orchestration Mongo + App
├── README.md                   ← Ce fichier
├── app/
│   ├── Dockerfile              ← Image de l'application
│   ├── healthcare_mongo_loader_optimized.py
│   ├── requirements.txt
│   └── data/
│       └── healthcare_dataset.csv
└── test/
    └── test_healthcare_loader.py
```

---

## 🧭 Étapes de la migration

### 1. Connexion MongoDB

```python
MongoClient("mongodb://localhost:27017", tz_aware=True)
```

> Le flag `tz_aware` permet d’interagir avec des objets `datetime` conscients du fuseau horaire.

### 2. Création / Mise à jour de la collection

- Si la collection `Patients` n’existe pas → **création + schéma JSON appliqué**
- Si elle existe → **mise à jour du schéma sans suppression** via `collMod` :

```python
db.command({
  "collMod": "Patients",
  "validator": {"$jsonSchema": patient_schema},
  "validationLevel": "strict",
  "validationAction": "error"
})
```

### 3. Chargement du CSV

- Lecture `pandas`
- Nettoyage des champs texte : capitalisation, formatage sang/gender/type...
- Transformation explicite des types :
  - `int` (âge, room)
  - `float` (billing)
  - `pendulum.parse(...)` pour les dates → converties naïves UTC
- Insertion par batch :
  - Par défaut `batch_size = 2000`
  - Insertions via `bulk_write(InsertOne(...))` désordonnées (`ordered=False`)

### 4. Index MongoDB automatiques

| Champs                              | Type d’index | Utilité principale                               |
|------------------------------------|--------------|--------------------------------------------------|
| `["Date of Admission"]`            | Simple       | Requêtes temporelles, tendances hospitalières    |
| `["Discharge Date"]`               | Simple       | Durées moyennes, taux de sortie                  |
| `["Medical Condition"]`            | Simple       | Analyses santé par pathologie                    |
| `["Hospital", "Admission Type"]`   | Composé      | Requêtes par établissement et type d’admission   |
| `["Doctor"]`                       | Simple       | Regroupement par médecin                         |

> ❗ Pas d’unicité sur `"Name"` ou `"Name + Date"` pour éviter les faux positifs (doublons légitimes).

### 5. Journalisation avec `loguru`

- Logs en console + fichiers rotatifs (`500 KB`, retention 5 jours)
- Niveaux : `INFO`, `SUCCESS`, `WARNING`, `ERROR`

---

## ✅ Tests automatisés (avec `pytest + mongomock`)

```bash
make test
```

| Test                                      | Description                                                                 |
|------------------------------------------|-----------------------------------------------------------------------------|
| `test_create_schema`                     | Création et structure du schéma MongoDB                                    |
| `test_load_data_transformation`          | Transformation correcte des types et standardisation texte                  |
| `test_schema_rejects_invalid_doc`        | Rejet des documents invalides (ex : champ requis manquant)                 |
| `test_batch_logging_and_processing`      | Log et fonctionnement du traitement par lots                              |
| `test_malformed_csv_logs_error`          | Détection d’erreurs de type (ex : âge non entier)                          |
| `test_partial_schema_doc`                | Insertion avec champ en surplus (toléré)                                   |
| `test_gender_capitalization`             | Capitalisation cohérente des champs texte                                  |
| `test_date_parsing`                      | Conversion correcte des dates                                              |
| `test_data_integrity_against_schema`     | Colonnes CSV présentes, non nulles, cohérence Mongo                        |
| `test_no_null_fields_after_migration`    | Champs requis non null dans MongoDB                                        |
| `test_csv_and_mongo_count_match`         | Nombre de lignes insérées == lignes du CSV                                 |
| `test_empty_csv`                         | Fichier CSV vide logué proprement                                          |

---

## 🧪 Validation d'intégrité des données

- 📑 Colonnes présentes
- 🔢 Types conformes (`int`, `float`, `date`, `str`)
- 🚫 Champs requis non nuls
- ♻️ Absence de doublons (`Name` + `Date of Admission`)
- 🧼 Normalisation chaînes (`Gender`, `Blood Type`, etc.)

---

## 📦 Dockerisation

### Dockerfile

L’image installe :

* Python 3
* Les dépendances (pandas, pymongo, loguru, pendulum)
* Monte le volume `/app/data` contenant le CSV
* Exécute le script à l’entrée du conteneur

### docker-compose.yml

Lance deux services :

* `mongo` (conteneur MongoDB)
* `app` (notre script)

```yaml
volumes:
  mongo_data:
  csv_data:

services:
  mongo:
    image: mongo
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  app:
    build: ./app
    volumes:
      - ./app/data:/app/data
    depends_on:
      - mongo
    environment:
      - MONGO_URI=mongodb://mongo:27017
```

---

## ⚙️ Makefile

```makefile
build:
	docker compose build

up:
	docker compose up

down:
	docker compose down

test:
	pytest -v test/test_healthcare_loader.py
```

---

## 📂 Fichiers clés

| Fichier                            | Rôle                                                                 |
|-----------------------------------|----------------------------------------------------------------------|
| `healthcare_mongo_loader_optimized.py` | Script principal                                                     |
| `test_healthcare_loader.py`       | Suite de tests Pytest                                                |
| `Healthcare_Dataset_Dictionary.csv` | Dictionnaire des colonnes                                            |
| `Dockerfile`                      | Image de l’app                                                       |
| `docker-compose.yml`              | Orchestration Mongo + App                                            |
| `Makefile`                        | Automatisation                                                        |

---

## ▶️ Lancer la migration complète


```bash
make build
make up
```

* Vérifie que `healthcare_dataset.csv` est bien dans `app/data/`
* Tu peux suivre les logs :

  ```bash
  docker compose logs -f app
  ```


---

## 🧼 Nettoyage

```bash
make down
docker volume prune -f
```

---

## 📋 Notes techniques

* Insertion via `bulk_write` en mode **non ordonné** (`ordered=False`) pour :

  * améliorer les performances
  * éviter l’échec complet en cas d’erreur dans un document

* Les dates sont converties via `pendulum` avec UTC par défaut :

  ```python
  pendulum.parse(...).naive()
  ```

---

## 👨‍💻 Auteur

Rudy Desplan – Data Engineer
