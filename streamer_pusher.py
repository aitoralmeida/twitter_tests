import sys
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
        contents = open(filename).read()
    except:
        logger.error("Error reading %s; retrying in 1 second..." % filename)
        self.retry(countdown = 1)
        return

    try:
        headers = {'content-type': 'application/json'}
        r = requests.post(WEB_URL, data = contents, headers = headers, auth = (WEB_USER, WEB_PASSWORD)) # Already in JSON
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
