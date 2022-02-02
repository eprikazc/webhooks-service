1. Prepare Postgres database with following table:
```
CREATE TABLE webhook_attempt (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    url VARCHAR(1000),
    event_name VARCHAR(50),
    status_code INT NULL,
    error_details TEXT,
    will_retry BOOL,
    created_at TIMESTAMP DEFAULT now()
);
```
2. Run Redis server
3. Set environment variable with Postgres and Redis connection strings:
```
export DATABASE_URL="postgres://postgres@127.0.0.1/my_db"
export REDIS_URL="redis://127.0.0.1"
export REDIS_CHANNEL_NAME="channel:1"
```
4. Run webhooks service:
```
python run.py
```
5. Now, you can connect to Redis and send message to the channel. For example:
```
python send_example_message.py
```
