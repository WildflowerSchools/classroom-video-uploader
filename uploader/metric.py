import os

from telegraf.client import TelegrafClient


HOST = os.environ.get("TELEGRAF_HOST", 'localhost')


def emit(name, values, tags=None):
    client = TelegrafClient(host=HOST, port=8092)
    client.metric(name, values, tags=tags)
