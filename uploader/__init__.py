import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
import json
import logging
import os
import queue
import time
import uuid

from minio import Minio
from minio.error import MinioException
from redis import StrictRedis
import yaml

import video_io.client
import video_io.config

from uploader.json_serializer import json_serializer
from uploader.metric import emit


BOOT_CONFIG_PATH = os.environ.get("BOOT_CONFIG_PATH", "/boot/wildflower-config.yml")
with open(BOOT_CONFIG_PATH, "r", encoding="utf8") as fp:
    config = yaml.safe_load(fp.read())


ENVIRONMENT_ID = config.get("environment-id", "unassigned")

UPLOADER_MAX_WORKERS = os.environ.get(
    "UPLOADER_MAX_WORKERS", None
)  # Will default to ThreadPoolExecutor's preference

EVENTS_KEY = os.environ.get("EVENTS_KEY", "minio-video-events")
EVENTS_KEY_ACTIVE = f"{EVENTS_KEY}.active"
EVENTS_KEY_FAILED = f"{EVENTS_KEY}.failed"
BUCKET_NAME = os.environ.get("BUCKET_NAME", "videos")
REDIS_HOST = os.environ.get("UPLOADER_REDIS_HOST")
REDIS_PASSWORD = os.environ.get("UPLOADER_REDIS_PASSWORD", None)
REDIS_PORT = os.environ.get("UPLOADER_REDIS_PORT", 6379)
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_KEY = os.environ.get("MINIO_KEY")
MINIO_SECRET = os.environ.get("MINIO_SECRET")

# Set using ENV var VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY
VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY = (
    video_io.config.VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY
)

logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y/%m/%d %I:%M:%S %p",
    level=logging.DEBUG,
)


HERE = os.path.dirname(__file__)


class VideoUploadError(Exception):
    pass


def parse_duration(dur):
    if dur is None:
        return 10000
    if dur.endswith("ms"):
        return int(dur[:-2])
    if dur.endswith("s"):
        return int(dur[:-1]) * 1000
    if dur.endswith("m"):
        return int(dur[:-1]) * 1000 * 60
    return 0


def fix_ts(ts):
    if "_" in ts:
        dt = datetime.strptime(ts, "%Y_%m_%d_%H_%M-%S")
        return dt.isoformat() + "Z"
    return ts


def process_file(video_client, minio_client, redis, key=None):
    uid = uuid.uuid4().hex[:8]
    logging.info(f"{uid} - Attempting upload of {key} to video_io service...")

    if key:
        if hasattr(key, "endswith") and not (
            key.endswith("mp4") or key.endswith("h264")
        ):
            redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info(
                f"{uid} - Skipping {key}, file doesn't appear to be an mp4 or h264"
            )
            return

        temp_subpath = os.path.normpath(f"{ENVIRONMENT_ID}/{key}")
        temp_fullpath = os.path.normpath(
            f"{VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY}/{ENVIRONMENT_ID}/{key}"
        )
        try:
            logging.info(f"{uid} - Loading file '{BUCKET_NAME}/{key} 'from minio")
            minio_client.fget_object(BUCKET_NAME, key, temp_fullpath)
            logging.info(
                f"{uid} - Loaded '{BUCKET_NAME}/{key}' and stored temporarily at {temp_fullpath}"
            )

            logging.info(
                f"{uid} - Beginning upload of '{temp_fullpath}' to video_io service..."
            )
            try:
                loop = asyncio.new_event_loop()
                coroutine = video_client.upload_video(
                    path=temp_subpath,
                    local_cache_directory=VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY,
                )
                try:
                    response = loop.run_until_complete(asyncio.wait_for(coroutine, 30))
                except TimeoutError:
                    logging.error(f"{uid} - AsyncIO thread loop timeout, unable to upload video file to video_io service")
                    return

                if "error" in response:
                    raise VideoUploadError(
                        f"{uid} - Unable to upload video file to video_io service: {response}"
                    )

                logging.info(
                    f"{uid} - Successfully uploaded '{temp_fullpath}' to video_io service"
                )

                logging.info(f"{uid} - Removing '{BUCKET_NAME}/{key}' from Minio...")
                minio_client.remove_object(BUCKET_NAME, key)
                logging.info(f"{uid} - Removed '{BUCKET_NAME}/{key}' from Minio")

                emit(
                    "wf_camera_uploader",
                    {"success": 1},
                    {"uid": uid, "environment": ENVIRONMENT_ID, "type": "success"},
                )

                if redis.hexists(EVENTS_KEY_FAILED, key):
                    redis.hdel(EVENTS_KEY_FAILED, key)
            except Exception as e:
                emit(
                    "wf_camera_uploader",
                    {"fail": 1},
                    {"uid": uid, "environment": ENVIRONMENT_ID, "type": "error"},
                )

                failed_value = {
                    "last_failed_at": datetime.utcnow(),
                    "failed_count": 0
                }
                if redis.hexists(EVENTS_KEY_FAILED, key):
                    failed_value = json.loads(redis.hget(EVENTS_KEY_FAILED, key))

                failed_value['last_failed_at'] = datetime.utcnow()
                failed_value['failed_count'] += 1
                if failed_value['failed_count'] > 3:
                    logging.warning(f"{uid} - Failed uploading too many times, removing video for {key}")

                    redis.hdel(EVENTS_KEY_FAILED, key)

                    logging.warning(f"{uid} - Removing '{BUCKET_NAME}/{key}' from Minio...")
                    minio_client.remove_object(BUCKET_NAME, key)
                    logging.warning(f"{uid} - Removed '{BUCKET_NAME}/{key}' from Minio")
                else:
                    logging.warning(f"{uid} - Failed uploading '{key}' - attempt #{failed_value['failed_count']}, adding to the failed retry queue")
                    redis.hset(EVENTS_KEY_FAILED, key, json.dumps(failed_value, default=json_serializer))

                raise e
        except VideoUploadError as e:
            logging.error(f"{uid} - Failed uploading '{key}': {e}")
        except MinioException as e:
            # this was probably a re-queue of a failed delete.
            logging.error(
                f"{uid} - Could not remove '{key}' from minio, unable to delete file from Minio service: {e}"
            )
        except Exception as e:
            logging.error(
                f"{uid} - Unexpected error attempting to upload '{key}' to video_io service: {e}"
            )
        finally:
            logging.info(f"{uid} - Removing '{key}' from Redis cache...")
            res = redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info(f"{uid} - Removed '{key}' from Redis cache: {res}")

        os.unlink(temp_fullpath)


def get_redis():
    return StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)


def get_minio_client():
    return Minio(
        MINIO_HOST, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False
    )


def key_generator(redis):
    with open(os.path.join(HERE, "next.lua"), "r", encoding="utf8") as nfp:
        next_script = redis.register_script(nfp.read())

    while True:
        try:
            # I think this may be crap, sort of looks like it raises an error if the hash is empty
            key = next_script(args=[EVENTS_KEY]).decode("utf-8")
        except Exception as e:
            logging.error(f"Failed to load next key from Redis cache: {e}")
            key = None
            time.sleep(2)

        yield key


def main():
    logging.debug("Uploader starting up")

    redis = get_redis()
    minio_client = get_minio_client()
    video_client = video_io.client.VideoStorageClient()

    max_workers = (
        int(UPLOADER_MAX_WORKERS)
        if UPLOADER_MAX_WORKERS is not None and UPLOADER_MAX_WORKERS.isdigit()
        else None
    )

    partial_process_file = partial(
        process_file, video_client=video_client, minio_client=minio_client, redis=redis
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # pylint: disable=W0212
        logging.info(f"Running uploader with {executor._max_workers} workers")
        q = queue.Queue(maxsize=executor._max_workers)

        while True:
            for key in key_generator(redis):
                if key is None:
                    logging.warning(
                        "Next Redis key returned was 'None', assuming queue is empty"
                    )
                    continue

                # Use a queue in order to block the loop and keep threadpoolexecutor from creating
                # futures for all tasks. We want to do this to keep the lua script from putting
                # everything on the "active" list. The queue will fill up, block, and as thread futures
                # complete the queue is cleaned up
                q.put(1, block=True)
                future = executor.submit(lambda k: partial_process_file(key=k), key)
                future.add_done_callback(q.get)


if __name__ == "__main__":
    main()
