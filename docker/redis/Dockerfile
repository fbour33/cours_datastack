# Utilisez une image de base avec Python
FROM python:3.8

ENV PYTHONUNBUFFERED=1

# Copiez le script Python dans l'image
COPY ./2_redis_sync.py /app/2_redis_sync.py

# Installez les dépendances
RUN pip install minio pika redis
 
# Définissez le répertoire de travail
WORKDIR /app

# Définissez la commande à exécuter lors du lancement de l'image
ENTRYPOINT python -u 2_redis_sync.py