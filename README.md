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
|🔐|Initialisation automatique et idempotente des rôles et utilisateurs (loader, analyst)|
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
Root/
├── app/
│   ├── data/
│   │   ├── healthcare_dataset.csv
│   │   └── Healthcare_Dataset_Dictionary.csv
│   ├── test/
│   │   └── test_healthcare_loader.py
│   ├── Dockerfile
│   ├── healthcare_mongo_loader_optimized.py
│   └── requirements.txt
├── docs/
├── docker-compose.yml
├── Makefile
└── README.md
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

1. Connexion admin initiale : Le script se connecte d'abord à Mongo en tant qu'utilisateur root via une URI dédiée (--admin_mongo_uri).

2. Initialisation des Rôles & Utilisateurs : Il exécute une fonction idempotente (initialize_mongodb_users_and_roles) qui crée les rôles (loaderRole, analystRole) et les utilisateurs (loader, analyst) s'ils n'existent pas.

3. Connexion loader : Le script se déconnecte puis se reconnecte avec l'utilisateur loader, qui a des privilèges limités, respectant ainsi le principe du moindre privilège.

4. Application des Schémas & Index : Création ou mise à jour des validateurs de schéma (collMod) et des index pour les 4 collections.

5. Lecture CSV par Chunks : Lecture du fichier CSV par lots (taille par défaut : 5 000).

6. Nettoyage & Validation : Conversions de types, nettoyage de texte et validation vectorisée des données patients via pandas.

7. Bulk-Upsert/Insert :

	- Les Patients sont insérés/mis à jour via UpdateOne en mode upsert.

	- Les documents liés (Admissions, MedicalRecords, Billing) sont insérés via InsertOne.

	- Toutes les opérations sont ordonnées : en cas d'erreur de validation, le lot est interrompu et le script tente d'insérer les documents restants un par un.

8. Logs Détaillés : Journalisation des succès et des erreurs pour chaque lot.

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
version: '3.9'

services:
  mongodb:
    image: mongo:8.0
    container_name: mongodb
    restart: unless-stopped
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_ROOT_USERNAME=root
      - MONGO_INITDB_ROOT_PASSWORD=rootpwd
      - MONGO_INITDB_DATABASE=HealthcareDB
    volumes:
      - mongo-data:/data/db
    networks:
      - healthcare-net

  app:
    build:
      context: ./app
      dockerfile: Dockerfile
    container_name: healthcare_loader
    depends_on:
      - mongodb
    volumes:
      - csv-data:/app/data
    networks:
      - healthcare-net
    environment:
      - PYTHONUNBUFFERED=1
      # NO LONGER NEEDED HERE if hardcoding in command, but keeping for clarity
      # - MONGO_URI=mongodb://loader:loaderpwd@mongo:27017/HealthcareDB?authSource=HealthcareDB
      # - ADMIN_MONGO_URI=mongodb://root:rootpwd@mongo:27017/admin?authSource=admin
    command: >
      bash -c "
      sleep 15 &&
      python healthcare_mongo_loader_optimized.py \
      --csv /app/data/healthcare_dataset.csv \
      --mongo_uri 'mongodb://loader:loaderpwd@mongodb:27017/HealthcareDB?authSource=HealthcareDB' \
      --db_name HealthcareDB \
      --admin_mongo_uri 'mongodb://root:rootpwd@mongodb:27017/admin?authSource=admin'
      "

volumes:
  mongo-data:
  csv-data:

networks:
  healthcare-net:
    driver: bridge
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

### 1️⃣  Initialisation automatisée des rôles & utilisateurs MongoDB

1️⃣ Initialisation automatisée des rôles & utilisateurs

Le script healthcare_mongo_loader_optimized.py gère désormais automatiquement la création des rôles et des utilisateurs nécessaires lors de son premier lancement.

Cette initialisation est 

idempotente : si un rôle ou un utilisateur existe déjà, sa création est simplement ignorée, évitant ainsi les erreurs lors de lancements multiples.

| Rôle Mongo / Utilisateur          | Privilèges précis sur `HealthcareDB`                                     | Pourquoi / périmètre d’usage                                 |
|-----------------------------------|---------------------------------------------------------------------------|---------------------------------------------------------------|
| `loaderRole` (utilisateur **loader**)  | `find`, `insert`, `update`, `createIndex`, `collMod` sur *toutes* les collections | Pipeline d’ingestion : insère des documents, gère les index & validation JSON |
| `analystRole` (utilisateur **analyst**) | `find` (lecture seule) sur *toutes* les collections                       | BI, dashboards, consultation des données                      |
| *(rôle natif)* **admin** (utilisateur **admin**) | `dbAdmin` + `userAdmin`                                                   | Gestion des schémas, index, utilisateurs & rôles              |

---

> 2️⃣  Authentification & Connexion de l’application

Le script utilise maintenant deux URIs de connexion distinctes, passées en arguments, pour séparer les tâches :

1. URI Admin (--admin_mongo_uri) : Utilisée une seule fois au démarrage pour se connecter en tant que root et exécuter la création des rôles/utilisateurs.

	- Exemple : mongodb://root:rootpwd@mongo:27017/admin

2. URI Loader (--mongo_uri) : Utilisée pour toutes les opérations de chargement de données avec l'utilisateur aux droits restreints loader.

	- Exemple : mongodb://loader:loaderpwd@mongo:27017/HealthcareDB?authSource=HealthcareDB

Ce découplage est une bonne pratique de sécurité qui garantit que l'application n'utilise les pleins pouvoirs que lorsque c'est strictement nécessaire.

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