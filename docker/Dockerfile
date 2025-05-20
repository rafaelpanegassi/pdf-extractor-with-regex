FROM docker.io/library/python:3.12.0-slim AS base

RUN apt-get update && apt-get install --yes \
    build-essential \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6
    
RUN pip install poetry==1.7.1

ENV DB_NAME $DB_NAME
ENV DB_USER $DB_USER
ENV DB_PASSWORD $DB_PASSWORD
ENV DB_HOST $DB_HOST
ENV AWS_BUCKET $AWS_BUCKET
ENV AWS_REGION $AWS_REGION
ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ENV QUEUE_NAME $QUEUE_NAME
ENV HTML_QUEUE_NAME $HTML_QUEUE_NAME

WORKDIR /usr/app/
COPY pyproject.toml /usr/app/

RUN poetry config virtualenvs.create false
RUN poetry install

COPY . .

# Defina o diretório de trabalho para o diretório src
WORKDIR /usr/app/src

CMD ["python3", "__init__.py"]