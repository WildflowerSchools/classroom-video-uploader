import asyncio
from datetime import datetime
import logging
import os
import time

from minio import Minio
from minio.error import MinioException
from redis import StrictRedis
import yaml

import video_io.client
import video_io.config

from uploader.metric import emit


BOOT_CONFIG_PATH = os.environ.get("BOOT_CONFIG_PATH", '/boot/wildflower-config.yml')
with open(BOOT_CONFIG_PATH, 'r', encoding="utf8") as fp:
    config = yaml.safe_load(fp.read())


ENVIRONMENT_ID = config.get("environment-id", "unassigned")

EVENTS_KEY = os.environ.get("EVENTS_KEY", 'minio-video-events')
EVENTS_KEY_ACTIVE = f"{EVENTS_KEY}.active"
BUCKET_NAME = os.environ.get("BUCKET_NAME", 'videos')
REDIS_HOST = os.environ.get("UPLOADER_REDIS_HOST")
REDIS_PASSWORD = os.environ.get("UPLOADER_REDIS_PASSWORD", None)
REDIS_PORT = os.environ.get("UPLOADER_REDIS_PORT", 6379)
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_KEY = os.environ.get("MINIO_KEY")
MINIO_SECRET = os.environ.get("MINIO_SECRET")

# Set using ENV var VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY
VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY = video_io.config.VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.DEBUG)


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
        return dt.isoformat() + 'Z'
    return ts


def process_file(video_client, minioClient, redis, next_script):
    try:
        # I think this may be crap, sort of looks like it raises an error if the hash is empty
        key = next_script(args=[EVENTS_KEY]).decode("utf-8")
    except Exception as e:
        logging.error(f"Failed to load next key from Redis cache: {e}")
        key = None
        time.sleep(1)

    if key:
        logging.info(f"Attempting upload of {key} to video_io service...")
        if hasattr(key, "endswith") and not (key.endswith("mp4") or key.endswith("h264")):
            redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info(f"Skipping {key}, file doesn't appear to be an mp4 or h264")
            return

        temp_subpath = os.path.normpath(f"{ENVIRONMENT_ID}/{key}")
        temp_fullpath = os.path.normpath(f"{VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY}/{ENVIRONMENT_ID}/{key}")
        try:
            logging.info(f"Loading file '{BUCKET_NAME}/{key} 'from minio")
            minioClient.fget_object(BUCKET_NAME, key, temp_fullpath)
            logging.info(f"Loaded '{BUCKET_NAME}/{key}' and stored temporarily at {temp_fullpath}")

            logging.info(f"Beginning upload of '{temp_fullpath}' to video_io service...")
            try:

                upload_task = video_client.upload_video(path=temp_subpath, local_cache_directory=VIDEO_STORAGE_LOCAL_CACHE_DIRECTORY)
                response = await asyncio.wait_for(upload_task, timeout=10)
                if 'error' in response:
                    raise VideoUploadError(f"Unable to upload video file to video_io service: {response}")

                logging.info(f"Successfully uploaded '{temp_fullpath}' to video_io service")

                logging.info(f"Removing '{BUCKET_NAME}/{key}' from Minio...")
                minioClient.remove_object(BUCKET_NAME, key)
                logging.info(f"Removed '{BUCKET_NAME}/{key}' from Minio")

                emit('wf_camera_uploader', {"success": 1}, {"environment": ENVIRONMENT_ID, "type": "success"})
            except asyncio.TimeoutError:
                logging.error("Failed writing video to video_io service because of timeout, video will be requeued for upload")
            except Exception as e:
                emit('wf_camera_uploader', {"fail": 1}, {"environment": ENVIRONMENT_ID, "type": "error"})
                raise e
        except VideoUploadError as e:
            logging.error(f"Failed uploading '{key}': {e}")
        except MinioException as e:
            # this was probably a re-queue of a failed delete.
            logging.error(f"Could not remove '{key}' from minio, unable to delete file from Minio service: {e}")
        except Exception as e:
            logging.error(f"Unexpected error attempting to upload '{key}' to video_io service: {e}")
        finally:
            logging.info(f"Removing '{key}' from Redis cache...")
            res = redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info(f"Removed '{key}' from Redis cache: {res}")

        os.unlink(temp_fullpath)


def get_redis():
    return StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)


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
            logging.error(f"Upload failed: {e}")
            from traceback import print_exc
            print_exc()


if __name__ == '__main__':
    main()
