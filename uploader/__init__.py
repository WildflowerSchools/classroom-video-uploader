from datetime import datetime
import logging
import os
import tempfile
import time
# import urllib.parse

from minio import Minio
from minio.error import NoSuchKey
from redis import StrictRedis

from honeycomb import HoneycombClient
from honeycomb.models import DatapointInput, S3FileInput


EVENTS_KEY = os.environ.get("EVENTS_KEY", 'minio-video-events')
EVENTS_KEY_ACTIVE = "%s.active" % EVENTS_KEY
BUCKET_NAME = os.environ.get("BUCKET_NAME", 'videos')
REDIS_HOST = os.environ.get("UPLOADER_REDIS_HOST")
REDIS_PORT = os.environ.get("UPLOADER_REDIS_PORT", 6379)
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_KEY = os.environ.get("MINIO_KEY")
MINIO_SECRET = os.environ.get("MINIO_SECRET")
HONECOMB_CLIENT_ID = os.environ.get("HONECOMB_CLIENT_ID")
HONECOMB_CLIENT_SECRET = os.environ.get("HONECOMB_CLIENT_SECRET")

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.DEBUG)


HERE = os.path.dirname(__file__)


def parse_duration(dur):
    if dur is None:
        return 10000
    if dur.endswith("ms"):
        return int(dur[:-2])
    elif dur.endswith("s"):
        return int(dur[:-1]) * 1000
    elif dur.endswith("m"):
        return int(dur[:-1]) * 1000 * 60
    return 0


def fix_ts(ts):
    if "_" in ts:
        dt = datetime.strptime(ts, "%Y_%m_%d_%H_%M-%S")
        return dt.isoformat() + 'Z'
    else:
        return ts


def process_file(honeycomb_client, minioClient, redis, next_script):
    try:
        # I think this may be crap, sort of looks like it raises and error if the hash is empty
        key = next_script(args=[EVENTS_KEY])
    except Exception:
        key = None
        time.sleep(1)

    if key:
        logging.info("uploading %s", key)
        if hasattr(key, "endswith") and not key.endswith(b"mp4"):
            redis.hdel(EVENTS_KEY_ACTIVE, key)
            return
        temp = tempfile.NamedTemporaryFile(delete=False)
        try:
            data = minioClient.fget_object(BUCKET_NAME, key, temp.name)
            assignment_id = data.metadata.get("X-Amz-Meta-Source")
            duration = parse_duration(data.metadata.get("X-Amz-Meta-Duration"))
            ts = fix_ts(data.metadata.get("X-Amz-Meta-Ts"))
            temp.flush()
            temp.close()
            with open(temp.name, 'rb') as fp:
                file_contents = fp.read()
                dp = DatapointInput(
                    observer=assignment_id,
                    format="video/mp4",
                    duration=duration,
                    file=S3FileInput(
                        name=key,
                        contentType="video/mp4",
                        data=file_contents,
                    ),
                    observed_time=ts,
                )
                try:
                    response = honeycomb_client.mutation.createDatapoint(dp)
                    logging.info("--------------------------------------------------------")
                    logging.info(response.to_json())
                    logging.info("--------------------------------------------------------")
                    # minioClient.remove_object(BUCKET_NAME, key)
                    res = redis.hdel(EVENTS_KEY_ACTIVE, key)
                    logging.info("%s removed from active list %s", key, res)
                except Exception:
                    logging.error("createDatapoint failed")
        except NoSuchKey:
            # this was probably a re-queue of a failed delete.
            logging.info("%s no longer in minio", key)
            res = redis.hdel(EVENTS_KEY_ACTIVE, key)
            logging.info("%s removed from active list %s", key, res)
        temp.close()
        os.unlink(temp.name)


def get_redis():
    return StrictRedis(host=REDIS_HOST, port=REDIS_PORT)


def get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)


def main():
    logging.debug("uploader starting up")
    logging.debug("received settings:")
    logging.debug("  EVENTS_KEY:              %s", EVENTS_KEY)
    logging.debug("  BUCKET_NAME:             %s", BUCKET_NAME)
    logging.debug("  REDIS_HOST:              %s", REDIS_HOST)
    logging.debug("  REDIS_PORT:              %s", REDIS_PORT)
    logging.debug("  MINIO_HOST:              %s", MINIO_HOST)
    logging.debug("  MINIO_KEY:               %s", MINIO_KEY)
    logging.debug("  MINIO_SECRET:            %s", MINIO_SECRET)
    logging.debug("  HONECOMB_CLIENT_ID:      %s", HONECOMB_CLIENT_ID)
    logging.debug("  HONECOMB_CLIENT_SECRET:  %s", HONECOMB_CLIENT_SECRET)

    redis = get_redis()
    minioClient = get_minio_client()

    with open(os.path.join(HERE, "next.lua"), 'r') as nfp:
        next_script = redis.register_script(nfp.read())

    client_credentials = {
        "token_uri": "https://wildflowerschools.auth0.com/oauth/token",
        "audience": "https://honeycomb.api.wildflowerschools.org",
        "client_id": HONECOMB_CLIENT_ID,
        "client_secret": HONECOMB_CLIENT_SECRET,
    }
    honeycomb_client = HoneycombClient(uri="https://honeycomb.api.wildflower-tech.org/graphql", client_credentials=client_credentials)

    while True:
        process_file(honeycomb_client, minioClient, redis, next_script)


if __name__ == '__main__':
    main()
