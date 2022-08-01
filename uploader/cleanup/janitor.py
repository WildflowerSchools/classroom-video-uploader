import logging
import os
import time
from subprocess import run

import yaml

from uploader.metric import emit
from uploader import get_redis, get_minio_client, EVENTS_KEY, EVENTS_KEY_ACTIVE, BUCKET_NAME


with open('/boot/wildflower-config.yml', 'r', encoding="utf8") as fp:
    config = yaml.safe_load(fp.read())


ENVIRONMENT_ID = config.get("environment-id", "unassigned")
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", 1000))


def capture_disk_usage_stats(path="/videos"):
    resp = run(["du", "-d", "1", path], capture_output=True, check=False)
    lines = resp.stdout.decode('utf8').split('\n')
    values = {}
    for line in lines:
        if len(line):
            size, path = line.split('\t')
            if path[0] == "/":
                path = path[1:]
            values[path] = int(size)
    emit('wf_camera_uploader', values, {"environment": ENVIRONMENT_ID, "type": "disk_usage"})


def cleanup_active():
    """Looks at the active list and puts items back on
    queue if they have become stale. To determine if
    they are stale it reads the list and caches the
    id's. On the next check if the ids are still
    there it is put back on the queue.
    """
    redis = get_redis()
    minioClient = get_minio_client()
    old_keys = set()
    while True:
        keys = redis.hkeys(EVENTS_KEY_ACTIVE)
        logging.info("loaded active keys, %s found", len(keys))
        key_cache = set()
        rcnt = 0
        ncnt = 0
        for key in keys:
            if key in old_keys:
                try:
                    minioClient.stat_object(BUCKET_NAME, key)
                    rcnt += 1
                    value = redis.hget(EVENTS_KEY_ACTIVE, key)
                    redis.hset(EVENTS_KEY, key, value)
                    redis.hdel(EVENTS_KEY_ACTIVE, key)
                except Exception:
                    redis.hdel(EVENTS_KEY_ACTIVE, key)
            else:
                key_cache.add(key)
                ncnt += 1
        old_keys = key_cache
        logging.info("%s removed from queue, %s newly seen", rcnt, ncnt)
        emit('wf_camera_uploader', {"removed": rcnt, "new": ncnt, "queue": len(keys) - rcnt}, {"environment": ENVIRONMENT_ID, "type": "cleanup"})
        capture_disk_usage_stats()
        time.sleep(60)


def queue_missed():
    """Lists objects in minio and finds items that are
    older than an hour and queues them if they have
    not been queued. Will only add if the queue is
    shorter than MAX_QUEUE (default: 1000).
    """
    redis = get_redis()
    minioClient = get_minio_client()
    while True:
        qlen = redis.hlen(EVENTS_KEY)
        logging.info("queue has %s items in it", qlen)
        if qlen < MAX_QUEUE:
            objects = list(minioClient.list_objects(BUCKET_NAME))
            objects = [obj.object_name for obj in objects if obj.object_name != "frames"]
            if len(objects):
                limit = min(100, (MAX_QUEUE - qlen) / len(objects))
                for obj in objects:
                    name = obj
                    logging.info("inspecting %s to add items to queue", name)
                    qlen += find_files(name, redis, minioClient, limit)
        emit('wf_camera_uploader', {"queue": qlen}, {"environment": ENVIRONMENT_ID, "type": "monitor"})
        time.sleep(30)


def find_files(name, redis, minioClient, limit):
    objects = minioClient.list_objects(BUCKET_NAME, prefix=name, recursive=True)
    count = 0
    for obj in objects:
        if count >= limit:
            break
        key = obj.object_name
        redis.hset(EVENTS_KEY, key, key)
        print(key)
        count += 1
    return count
