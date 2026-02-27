FROM python:3.11-slim

# On définit le dossier de travail à l'intérieur du conteneur. Toutes les commandes suivantes seront exécutées à partir de ce répertoire. Cela permet d'organiser les fichiers de l'application de manière cohérente dans l'image Docker.
WORKDIR /app

# On copie d'abord requirements.txt pour profiter du cache de Docker lors de l'installation des dépendances.
COPY requirements.txt .

# On installe les dépendances Python dans l'image en utilisant pip. L'option --no-cache-dir permet de ne pas stocker les fichiers temporaires d'installation, ce qui réduit la taille de l'image finale.
RUN pip install --no-cache-dir -r requirements.txt

# On copie le script de migration dans l'image Docker. Ce script sera exécuté au démarrage du conteneur pour effectuer les migrations de la base de données.
COPY migrate.py .

# Commande exécutée au démarrage du conteneur pour lancer le script de migration. Cela garantit que les migrations sont appliquées chaque fois que le conteneur est démarré.
CMD ["python", "migrate.py"]
