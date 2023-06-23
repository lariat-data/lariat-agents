import os

import redis


def get_default_redis_conn():
    return redis.Redis(
        host=os.environ.get(
            "REDIS_HOST",
            "localhost",
        ),
        port=os.environ.get("REDIS_PORT", "6379"),
        password=os.environ.get("REDIS_PASSWORD", ""),
    )


def delete_keys_with_prefix(redis_conn, prefix: str):
    cursor = 0
    while True:
        cursor, keys = redis_conn.scan(cursor, match=prefix + "*", count=1000)
        for key in keys:
            redis_conn.delete(key)
        if cursor == 0:
            break
