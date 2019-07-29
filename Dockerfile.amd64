FROM python:2.7-alpine

RUN apk add --update alpine-sdk glib glib-dev linux-headers

RUN mkdir /app

RUN pip install git+https://github.com/WildflowerSchools/graphql-python-client-generator.git
RUN pip install git+https://github.com/WildflowerSchools/wildflower-honeycomb-sdk-py.git

COPY ./uploader /app/uploader

RUN pip install -r /app/uploader/requirements.txt

WORKDIR /app

