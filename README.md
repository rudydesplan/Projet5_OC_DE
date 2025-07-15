Below is the **complete, up-to-date `README.md`** with an expanded **🔐 Contrôle d’accès / Sécurité (CE3)** section that shows *exact* Mongo Shell / `mongosh` commands to créer les utilisateurs, attribuer les rôles et se connecter depuis le conteneur `app`.

````markdown
# 🏥 Healthcare CSV → MongoDB Loader (v2)

Ce projet fournit un pipeline **robuste, testé, dockerisé et extensible** pour charger des données patients depuis un CSV vers MongoDB.

Il suit un **développement par branches** avec revue des fonctionnalités avant merge.

- **main** — branche de production
- **dev** — branche de développement continu
- **remote-testing** — branche dédiée aux tests d'intégration à distance

📅 **Historique de commits actif sur ≥ 2 semaines**  
💬 Visualisez le graphe réseau complet ici :  
[https://github.com/rudydesplan/Projet5_OC_DE/network](https://github.com/rudydesplan/Projet5_OC_DE/network)

![GitHub Network Graph](https://github.com/rudydesplan/Projet5_OC_DE/raw/main/docs/github-network.png)

---

> 💡 *L'image ci-dessus est générée depuis l'onglet **Insights → Network** de GitHub.*  
> Elle prouve l’usage actif de branches, de merges et de commits continus.
---

## 🚀 Fonctionnalités clés

| ✔  | Détail |
|----|--------|
|🗄️|**Schémas JSON** stricts sur 4 collections (Patients, Admissions, MedicalRecords, Billing)|
|⚡|Insertion **batch** via `bulk_write` (mode *ordonné* ⇒ arrêt sur première erreur, reprise automatique)|
|🔍|**Index** composés et simples créés automatiquement|
|🧽|Nettoyage & validation **vectorisés** (`pandas`) avant insertion|
|📝|**Logs** détaillés (rotation 500 KB / 5 jours) via `loguru`|
|🧪|Tests **unitaires** isolés (`pytest` + `mongomock`)|
|🐳|Déploiement **Docker / Docker-Compose** (Mongo + App)|
|🛠|Commandes **Makefile** “build / up / down / test”|
|🔐|Section Sécurité : **auth Mongo, rôles, commandes de création, connexion**|

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

## 🧩 Schémas JSON appliqués / mis à jour  

| Collection         | Champs (clés)                                                       | Contraintes principales*                                                                                                      |
|--------------------|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| **Patients**       | `Name`, `Age`, `Gender`, `Blood Type`, `Medical Condition`          | `Name : string` · `Age : long ∈ [0-125]` · `Gender ∈ {Male, Female}` · `Blood Type ∈ {A±, B±, O±, AB±}` · `Medical Condition ∈ {Cancer, Obesity, Diabetes, Asthma, Hypertension, Arthritis}` |
| **Admissions**     | `patient_id`, `Date of Admission`, `Admission Type`, `Room Number`, `Discharge Date` | `patient_id : objectId` · `Date of Admission : date` · `Admission Type ∈ {Urgent, Emergency, Elective, null}` · `Room Number : long ≥ 1 | null` · `Discharge Date : date | null` |
| **MedicalRecords** | `patient_id`, `Doctor`, `Hospital`, `Medication`, `Test Results`    | `patient_id : objectId` · `Doctor : string` · `Hospital : string` · `Medication : string | null` · `Test Results ∈ {Normal, Abnormal, Inconclusive, null}` |
| **Billing**        | `patient_id`, `Billing Amount`, `Insurance Provider`                | `patient_id : objectId` · `Billing Amount : double ≥ 0 | null` · `Insurance Provider : string | null`                                                              |

\*Table simplifiée ; les définitions complètes (titres, types BSON, `required`, bornes, etc.) se trouvent dans `healthcare_mongo_loader_optimized.py`.

> Les schémas sont appliqués via `collMod` (si la collection existe) ou lors de la création (`validationLevel: "strict"`, `validationAction: "error"`).

---

📊 **Diagramme entité-relation des collections**

![MongoDB Schema Diagram](https://github.com/rudydesplan/Projet5_OC_DE/raw/main/docs/diagram.png)

*Ce diagramme montre les relations entre la collection centrale `Patients` et les collections secondaires liées (`Admissions`, `MedicalRecords`, `Billing`).*

## ⚡ Index créés automatiquement

| Collection     | Index                                         | Pourquoi un « 1 » ?*                                  |
| -------------- | --------------------------------------------- | ----------------------------------------------------- |
| Patients       | `[(Name,1)…(Medical Condition,1)]` **unique** | `1` = *ordre croissant* (clé d’index Mongo classique) |
| Admissions     | `[(patient_id,1),(Date of Admission,1)]`      | Accès rapide par patient + période                    |
| MedicalRecords | `[(patient_id,1),(Doctor,1)]`                 | Requêtes patient-médecin                              |
| Billing        | `[(patient_id,1),(Billing Amount,1)]`         | Analyse facturation par patient                       |

\*Dans MongoDB un index se définit par (clé, direction) ; `1` = ASC, `-1` = DESC.

---

## 🧭 Flux de traitement

1. **Lecture** CSV en chunks (taille par défaut 5 000).
2. **Conversions numériques** (`Age`, `Room Number` → `Int64`; `Billing Amount` → `float64`).
3. **Nettoyage texte** (trim, upper/lower/title, NULL-likes).
4. **Validation vectorisée** (`validate_patients`) → masque True/False.
5. **Bulk-upsert** Patients (`ordered=True` : arrêt sur erreur, rejoue le reste unitaire).
6. **Récupération des `_id`** nouvellement créés.
7. **Préparation** & **bulk-insert** *ordonnés* des documents liés (Admissions / Medical / Billing).
8. `bypass_document_validation=False` : Mongo **revalide** chaque insert.
9. **Logs** succès / erreurs chunk par chunk.

---

## ✅ Tests automatisés (`pytest + mongomock`)

```bash
make test
```

Le jeu de tests vérifie :

| Test                              | Description                                                                                             |
|----------------------------------|---------------------------------------------------------------------------------------------------------|
| `test_create_schema`             | Création & structure complète des 4 schémas JSON MongoDB                                                |
| `test_load_data_transformation`  | Typage (long/int/double), mise en casse, normalisation des valeurs                                      |
| `test_schema_rejects_invalid_doc`| Injection d’un document invalide → rejet (simulateur mongomock)                                        |
| `test_batch_logging_and_processing`| Vérifie que les logs indiquent correctement la progression par lots                                    |
| `test_malformed_csv_logs_error`  | Fichier CSV mal formé → détection & log d’erreur                                                       |
| `test_partial_schema_doc`        | Insertion d’un doc avec champ surplus → toléré (non bloquant)                                          |
| `test_gender_capitalization`     | “male” → “Male” ; “FEMALE” → “Female”                                                                   |
| `test_date_parsing`              | Chaînes → `datetime` naïf (timezone locale ou UTC de la JVM mongomock)                                 |
| `test_data_integrity_against_schema`| Concordance colonnes CSV ↔︎ champs Mongo, non-nullité des requis                                      |
| `test_no_null_fields_after_migration`| Vérifie que les champs requis sont effectivement non null dans MongoDB                                |
| `test_csv_and_mongo_count_match` | Lignes insérées == lignes valides du CSV                                                                |
| `test_empty_csv`                 | CSV vide → warning loggué, aucune insertion                                                             |

---

## 📦 Dockerisation

### `Dockerfile` (app)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "healthcare_mongo_loader_optimized.py"]
```

### `docker-compose.yml`

```yaml
version: "3.8"

services:
  mongo:
    image: mongo:6
    restart: unless-stopped
    ports:
      - "27017:27017"
    environment:
      # Active l'auth dès le 1er lancement
      - MONGO_INITDB_ROOT_USERNAME=root
      - MONGO_INITDB_ROOT_PASSWORD=rootpwd
    volumes:
      - mongo_data:/data/db

  app:
    build: ./app
    depends_on: [mongo]
    environment:
      - MONGO_URI=mongodb://loader:loaderpwd@mongo:27017/HealthcareDB?authSource=HealthcareDB
    volumes:
      - ./app/data:/app/data
    command: >
      python healthcare_mongo_loader_optimized.py
      --csv /app/data/healthcare_dataset.csv
      --mongo_uri ${MONGO_URI}

volumes:
  mongo_data:
```

---

## ⚙️ Makefile

```makefile
build:          ## Build Docker images
	docker compose build

up:             ## Start stack (Mongo + App)
	docker compose up -d

logs:           ## Follow app logs
	docker compose logs -f app

down:           ## Stop & remove containers
	docker compose down

test:           ## Run pytest suite
	pytest -v test
```

---

## 🔐 Contrôle d’accès / Sécurité (CE3)

### 1️⃣  Création des rôles & utilisateurs MongoDB

| Rôle Mongo / Utilisateur | Privilèges précis sur `HealthcareDB` | Pourquoi / périmètre d’usage | Commande `mongosh` de création* |
|--------------------------|--------------------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `loaderRole` (utilisateur **loader**) | `insert`, `update`, `createIndex`, `collMod` sur *toutes* les collections | Pipeline d’ingestion : insère des documents, gère les index & validation JSON | ```js\nuse HealthcareDB\ndb.createRole({ role: "loaderRole", privileges: [{ resource: { db: "HealthcareDB", collection: "" }, actions: ["insert","update","createIndex","collMod"] }], roles: [] })\ndb.createUser({ user: "loader", pwd: "loaderpwd", roles: ["loaderRole"] })``` |
| `analystRole` (utilisateur **analyst**) | `find` (lecture seule) sur *toutes* les collections | BI, dashboards, consultation des données | ```js\nuse HealthcareDB\ndb.createRole({ role: "analystRole", privileges: [{ resource: { db: "HealthcareDB", collection: "" }, actions: ["find"] }], roles: [] })\ndb.createUser({ user: "analyst", pwd: "analystpwd", roles: ["analystRole"] })``` |
| *(rôle natif)* **admin** (utilisateur **admin**) | `dbAdmin` + `userAdmin` | Gestion des schémas, index, utilisateurs & rôles | ```js\nuse HealthcareDB\ndb.createUser({ user: "admin", pwd: "adminpwd", roles: [ { role: "dbAdmin", db: "HealthcareDB" }, { role: "userAdmin", db: "HealthcareDB" } ] })``` |

---

> Connectez-vous avec `mongosh` pour exécuter les commandes de création :
```bash
docker compose exec mongo mongosh -u root -p rootpwd --authenticationDatabase admin
```

**Extraits de commandes MongoDB**  
```javascript
use HealthcareDB

// Rôle loader
db.createRole({
  role: "loaderRole",
  privileges: [{ resource: { db: "HealthcareDB", collection: "" }, actions: ["insert", "update", "createIndex", "collMod"] }],
  roles: []
})

// Rôle analyst
db.createRole({
  role: "analystRole",
  privileges: [{ resource: { db: "HealthcareDB", collection: "" }, actions: ["find"] }],
  roles: []
})

// Utilisateurs
db.createUser({ user: "loader", pwd: "loaderpwd", roles: ["loaderRole"] })
db.createUser({ user: "analyst", pwd: "analystpwd", roles: ["analystRole"] })
db.createUser({
  user: "admin",
  pwd: "adminpwd",
  roles: [
    { role: "dbAdmin", db: "HealthcareDB" },
    { role: "userAdmin", db: "HealthcareDB" }
  ]
})
```

---

### 2️⃣  Authentification & Connexion de l’application

Le conteneur Python utilise l’URI suivante pour se connecter à MongoDB :  
```
mongodb://loader:loaderpwd@mongo:27017/HealthcareDB?authSource=HealthcareDB
```

Cette URI est fournie via la variable d’environnement `MONGO_URI` du `docker-compose.yml` et transmise au script via l’argument `--mongo_uri`.

---

### 3️⃣  Sécurité des mots de passe & recommandations 

✅ **Stockage sécurisé dans MongoDB**  
> MongoDB utilise par défaut **SCRAM-SHA-256** pour le stockage des mots de passe.  
> Il s’agit d’un protocole d’authentification sécurisé avec challenge-réponse et salage du hash, respectant les bonnes pratiques OWASP.

✅ **Pas de stockage en clair** dans les bases — ni dans le code.

---

### 4️⃣  Bonnes pratiques supplémentaires recommandées

| Mesure                                  | Implémentation actuelle / Recommandation |
|-----------------------------------------|------------------------------------------|
| **Transport sécurisé (TLS)**            | Ajouter `--tlsMode requireTLS` + certificats signés |
| **Secrets protégés**                    | Gérer les mots de passe via [Docker Secrets](https://docs.docker.com/engine/swarm/secrets/), fichiers `.env` non commités ou Hashicorp Vault |
| **Chiffrement des données au repos**    | Possible avec le moteur de stockage chiffré (MongoDB Enterprise) |
| **Sauvegardes régulières**              | Ex : `mongodump` via cronjob ou automatisation cloud |
| **Audit des accès**                     | Activer l’`auditLog` (si besoin de traçabilité fine) |

---

### 5️⃣  Exemple de fichier `.env` (recommandé)

```
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=rootpwd
LOADER_USER=loader
LOADER_PASS=loaderpwd
```

> Ces variables peuvent ensuite être injectées dans le `docker-compose.yml` ou via pipeline CI/CD.

---

✅ **Cette politique d’accès respecte le principe du moindre privilège**  
> Chaque utilisateur Mongo a **uniquement** les droits nécessaires à son rôle métier.


## ▶️ Lancer la migration complète

```bash
make build      # construit les images
make up         # démarre Mongo + loader (la migration s’exécute une fois)
make logs       # suit le stdout de l’app
```

Arrêt & nettoyage :

```bash
make down
docker volume prune -f   # ⚠️  supprime les données Mongo
```

---

## 👨‍💻 Auteur

Rudy Desplan – Data Engineer
````
