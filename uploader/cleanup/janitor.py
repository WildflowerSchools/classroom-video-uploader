import logging
import os
import time
from subprocess import run

import yaml

from uploader.metric import emit
from uploader import (
    get_redis,
    get_minio_client,
    EVENTS_KEY,
    EVENTS_KEY_ACTIVE,
    BUCKET_NAME,
)


BOOT_CONFIG_PATH = os.environ.get("BOOT_CONFIG_PATH", "/boot/wildflower-config.yml")
with open(BOOT_CONFIG_PATH, "r", encoding="utf8") as fp:
    config = yaml.safe_load(fp.read())


ENVIRONMENT_ID = config.get("environment-id", "unassigned")
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", 1000))


def capture_disk_usage_stats(path="/videos"):
    resp = run(["du", "-d", "1", path], capture_output=True, check=False)
    lines = resp.stdout.decode("utf8").split("\n")
    values = {}
    for line in lines:
        if len(line):
            size, path = line.split("\t")
            if path[0] == "/":
                path = path[1:]
            values[path] = int(size)
    emit(
        "wf_camera_uploader",
        values,
        {"environment": ENVIRONMENT_ID, "type": "disk_usage"},
    )


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
        logging.info(f"Loaded active keys. {len(keys)} keys found.")
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
        logging.info(f"{rcnt} removed from queue, {ncnt} newly seen")
        emit(
            "wf_camera_uploader",
            {"removed": rcnt, "new": ncnt, "queue": len(keys) - rcnt},
            {"environment": ENVIRONMENT_ID, "type": "cleanup"},
        )
        capture_disk_usage_stats()
        time.sleep(60)


def queue_missed():
    # The option to clear keys is helpful for debugging
    # clear_all_keys(EVENTS_KEY)

    """Lists objects in minio and queues items if they have
    not been queued. Will only add if the queue is
    shorter than MAX_QUEUE (default: 1000).
    """
    redis = get_redis()
    minioClient = get_minio_client()
    while True:
        qlen = redis.hlen(EVENTS_KEY)
        logging.info(
            f"Updating Redis queue. Queue contains {qlen} items before update..."
        )

        if qlen >= MAX_QUEUE:
            logging.info("Redis queue is full, not adding add'l items")
        if qlen < MAX_QUEUE:
            for obj in minioClient.list_objects(BUCKET_NAME, recursive=True):
                key = obj.object_name
                if redis.hexists(EVENTS_KEY, key) or redis.hexists(
                    EVENTS_KEY_ACTIVE, key
                ):
                    continue

                redis.hset(EVENTS_KEY, key, key)
                qlen += 1
                if qlen >= MAX_QUEUE:
                    break
        logging.info(f"Updated Redis queue. Queue now contains {qlen} items")
        emit(
            "wf_camera_uploader",
            {"queue": qlen},
            {"environment": ENVIRONMENT_ID, "type": "monitor"},
        )
        time.sleep(30)


def clear_all_keys(hash_name: None):
    redis = get_redis()

    if hash_name is None:
        return
    all_keys = list(redis.hgetall(hash_name).keys())
    if len(all_keys) > 0:
        redis.hdel(hash_name, *all_keys)
