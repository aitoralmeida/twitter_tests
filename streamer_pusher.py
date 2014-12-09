import bz2
import StringIO
import sys
import gzip
import json
import requests
from celery import Celery
from celery.utils.log import get_task_logger
cel = Celery('pusher_tasks', backend='amqp', broker='amqp://')

from config import WEB_USER, WEB_PASSWORD, WEB_URL

logger = get_task_logger(__name__)

@cel.task(name="push", bind = True, max_retries = None)
def push(self, filename):
    logger.info("Processing filename %s" % filename)
    try:
        contents = [ json.loads(line) for line in gzip.open(filename) ]
    except:
        logger.error("Error reading %s; retrying in 1 second..." % filename)
        self.retry(countdown = 1)
        return

    try:
        contents_bz2 = StringIO.StringIO(bz2.compress(json.dumps(contents)))
    except:
        logger.error("Error compressing %s; retrying in 10 second..." % filename)
        self.retry(countdown = 10)
        return

    try:
        r = requests.post(WEB_URL, files = { 'tweets.bz2' : contents_bz2}, auth = (WEB_USER, WEB_PASSWORD)) # Already in JSON
    except requests.exceptions.RequestException as re:
        logger.error("requests error: %s" % re)
        self.retry(countdown = 5)
    else:
        try:
            is_error = r.json().get('error', False)
        except Exception as e:
            logger.error("Invalid response: %s" % e)
            self.retry(countdown = 5)
        else:
            if is_error:
                logger.error("Application error: %s" % r.json())
                self.retry(countdown = 5)

if __name__ == '__main__':
    cel.worker_main(sys.argv)
