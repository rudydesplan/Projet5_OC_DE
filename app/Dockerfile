# ------------------------------
# Dockerfile for Healthcare Loader
# ------------------------------

# 1. Image de base légère avec Python
FROM python:3.13-slim

# 2. Définir un répertoire de travail dans le conteneur
WORKDIR /app

# 3. Copier les fichiers du projet dans l’image
COPY . /app

# 4. Installer les dépendances
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 5. Commande d’entrée (par défaut, on utilise le CSV dans /app/data)
CMD ["python", "healthcare_mongo_loader_optimized.py"]
