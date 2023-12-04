# Utilisez une image de base avec Python
FROM python:3.8

ENV PYTHONUNBUFFERED=1

# Copiez le script Python dans l'image
COPY ./1_rabbit_to_minio.py /app/1_rabbit_to_minio.py

# Installez les dépendances
RUN pip install minio pika
 
# Définissez le répertoire de travail
WORKDIR /app

# Définissez la commande à exécuter lors du lancement de l'image
ENTRYPOINT ["python", "1_rabbit_to_minio.py"]