import asyncio
from datetime import datetime
import logging
import os
import tempfile
import time

from minio import Minio
from minio.error import MinioException
from redis import StrictRedis
import yaml

import video_io.client

from uploader.metric import emit


with open('/boot/wildflower-config.yml', 'r', encoding="utf8") as config:
    config = yaml.safe_load(config.read())


ENVIRONMENT_ID = config.get("environment-id", "unassigned")

EVENTS_KEY = os.environ.get("EVENTS_KEY", 'minio-video-events')
EVENTS_KEY_ACTIVE = f"{EVENTS_KEY}.active"
BUCKET_NAME = os.environ.get("BUCKET_NAME", 'videos')
REDIS_HOST = os.environ.get("UPLOADER_REDIS_HOST")
REDIS_PORT = os.environ.get("UPLOADER_REDIS_PORT", 6379)
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_KEY = os.environ.get("MINIO_KEY")
MINIO_SECRET = os.environ.get("MINIO_SECRET")

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.DEBUG)


HERE = os.path.dirname(__file__)


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
        return dt.isoformat() + 'Z'
    return ts


def process_file(video_client, minioClient, redis, next_script):
    try:
        # I think this may be crap, sort of looks like it raises an error if the hash is empty
        key = next_script(args=[EVENTS_KEY]).decode("utf-8")
    except Exception:
        key = None
        time.sleep(1)

    if key:
        logging.info("uploading %s", key)
        if hasattr(key, "endswith") and not (key.endswith("mp4") or key.endswith("h264")):
            redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info("didn't look like an mp4 or h264")
            return
        temp_path = f"/data/{ENVIRONMENT_ID}/{key}"
        print(temp_path)
        try:
            logging.info("loading file from minio")
            minioClient.fget_object(BUCKET_NAME, key, temp_path)
            logging.info('file loaded from minio')
            logging.info("beginning upload")
            try:
                subpath = f"{ENVIRONMENT_ID}/{key}"
                logging.info(subpath)
                response = asyncio.run(video_client.upload_video(subpath))
                logging.info(response)
                minioClient.remove_object(BUCKET_NAME, key)
                res = redis.hdel(EVENTS_KEY_ACTIVE, key)
                logging.info("%s removed from active list %s", key, res)
                emit('wf_camera_uploader', {"success": 1}, {"environment": ENVIRONMENT_ID, "type": "success"})
            except Exception as e:
                emit('wf_camera_uploader', {"fail": 1}, {"environment": ENVIRONMENT_ID, "type": "error"})
                raise e
        except MinioException:
            # this was probably a re-queue of a failed delete.
            logging.info("%s no longer in minio", key)
            res = redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info("%s removed from active list %s", key, res)
        os.unlink(temp_path)


def get_redis():
    return StrictRedis(host=REDIS_HOST, port=REDIS_PORT)


def get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)


def main():
    logging.debug("uploader starting up")

    redis = get_redis()
    minioClient = get_minio_client()
    video_client = video_io.client.VideoStorageClient()

    with open(os.path.join(HERE, "next.lua"), 'r', encoding="utf8") as nfp:
        next_script = redis.register_script(nfp.read())

    while True:
        try:
            while True:
                process_file(video_client, minioClient, redis, next_script)
        except Exception as e:
            logging.error("upload failed, %s", e)
            from traceback import print_exc
            print_exc()


if __name__ == '__main__':
    main()
