import asyncio
import json
import os
import signal

import aiohttp
import aiohttp.client_exceptions
import aiopg
import aioredis
import backoff

db_conn_string = os.environ["DATABASE_URL"]
"""
Point to a database with following table:
"""
redis_conn_string = os.environ["REDIS_URL"]
redis_channel_name = os.environ["REDIS_CHANNEL_NAME"]


async def log_success(data):
    status_code, response_content = data["value"]
    kwargs = data["kwargs"]
    await _insert_webhook_attempt_to_db(
        kwargs["customer_id"],
        kwargs["url"],
        kwargs["event_name"],
        status_code,
        response_content,
        False,
    )


async def log_backoff(data):
    status_code, response_content = data["value"]
    kwargs = data["kwargs"]
    await _insert_webhook_attempt_to_db(
        kwargs["customer_id"],
        kwargs["url"],
        kwargs["event_name"],
        status_code,
        response_content,
        True,
    )


async def log_giveup(data):
    status_code, response_content = data["value"]
    kwargs = data["kwargs"]
    await _insert_webhook_attempt_to_db(
        kwargs["customer_id"],
        kwargs["url"],
        kwargs["event_name"],
        status_code,
        response_content,
        False,
    )


def should_retry(send_result):
    status_code, content = send_result
    return (status_code is None) or (500 <= status_code <= 599)


@backoff.on_predicate(
    backoff.constant,
    predicate=should_retry,
    interval=[2, 5, 10],
    on_success=log_success,
    on_backoff=log_backoff,
    on_giveup=log_giveup,
)
async def _send_request(customer_id: int, url: str, event_name: str, payload: dict):
    timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_connect=5, sock_read=5)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                url,
                json={"event_name": event_name, "payload": payload},
                timeout=timeout,
            ) as response:
                return (response.status, await response.content.read(50))
        except aiohttp.client_exceptions.ClientError as exc:
            return (None, str(exc))


async def redis_reader(channel: aioredis.client.PubSub):
    try:
        while True:
            message = await channel.get_message(
                timeout=2, ignore_subscribe_messages=True
            )
            if message is None:
                continue
            try:
                request_data = _get_request_data(message)
            except Exception:
                continue
            asyncio.create_task(
                _send_request(
                    customer_id=request_data["customer_id"],
                    url=request_data["url"],
                    event_name=request_data["event_name"],
                    payload=request_data["payload"],
                ),
                name="send_request_t",
            )
    except asyncio.exceptions.CancelledError:
        print("stopping")


def _get_request_data(redis_message: dict):
    data = json.loads(redis_message["data"])
    return {
        "customer_id": int(data["customer_id"]),
        "url": data["url"],
        "event_name": data["event_name"],
        "payload": data["payload"],
    }


async def _insert_webhook_attempt_to_db(
    customer_id: int,
    url: str,
    event_name: str,
    status_code: int,
    error_details: str,
    will_retry: bool,
):
    query = """
    INSERT INTO
        webhook_attempt
        (customer_id, url, event_name, status_code, error_details, will_retry)
        VALUES
        (%s, %s, %s, %s, %s, %s)
    """
    if isinstance(error_details, bytes):
        error_details = error_details.decode("utf-8")
    async with aiopg.connect(db_conn_string) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                query,
                (customer_id, url, event_name, status_code, error_details, will_retry),
            )


async def main():
    async def shutdown():
        future.cancel()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT, lambda: asyncio.create_task(shutdown(), name="shutdown")
    )
    redis = aioredis.from_url(redis_conn_string, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(redis_channel_name)

    future = asyncio.create_task(redis_reader(pubsub))
    await future
    # Let all pending requests to complete
    [await task for task in asyncio.all_tasks() if task.get_name() == "send_request_t"]


if __name__ == "__main__":
    asyncio.run(main())
