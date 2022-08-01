import logging
import time

from uploader import get_redis, EVENTS_KEY, EVENTS_KEY_ACTIVE


def monitor():
    """Looks at the active list and puts items back on
    queue if they have become stale. To determine if
    they are stale it reads the list and caches the
    id's. On the next check if the ids are still
    there it is put back on the queue.
    """
    redis = get_redis()
    while True:
        waiting = redis.hlen(EVENTS_KEY)
        active = redis.hlen(EVENTS_KEY_ACTIVE)
        logging.info("waiting keys, %s found", waiting)
        logging.info("active keys, %s found", active)
        logging.info("sending to influxdb")
        time.sleep(1)


if __name__ == '__main__':
    monitor()
