import json
import os

import redis

redis_url = os.environ["REDIS_URL"]
redis_channel_name = os.environ["REDIS_CHANNEL_NAME"]

r = redis.Redis.from_url(redis_url)
data = {
    "customer_id": 6,
    "url": "http://127.0.0.1:8000/test",
    "event_name": "feedback_created",
    "payload": {},
}
r.publish(redis_channel_name, json.dumps(data))
