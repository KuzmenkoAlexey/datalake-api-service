FROM python:3.9.10

RUN apt update && apt install -y exiftool

RUN mkdir -p /usr/app

WORKDIR /usr/app

COPY requirements.txt /usr/app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY src /usr/app

CMD uvicorn --host 0.0.0.0 main:app