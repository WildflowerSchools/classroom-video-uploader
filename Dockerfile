FROM python:3.10.10-slim


RUN mkdir /app
WORKDIR /app

RUN apt-get update
RUN apt-get install -y ffmpeg libsm6 libxext6

RUN pip install --upgrade pip opencv-contrib-python

COPY ./uploader /app/uploader

RUN pip install -r /app/uploader/requirements.txt

WORKDIR /app
