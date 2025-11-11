#!/usr/bin/env python3
"""Kafka consumer that writes enriched records into Snowflake."""

import json
import os

from kafka import KafkaConsumer  # type: ignore
import snowflake.connector  # type: ignore


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOW_ACCOUNT"],
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        warehouse=os.environ["SNOW_WAREHOUSE"],
        database=os.environ["SNOW_DATABASE"],
        schema=os.environ["SNOW_SCHEMA"],
        role=os.environ.get("SNOW_ROLE"),
    )


def main() -> None:
    consumer = KafkaConsumer(
        "quotes",
        bootstrap_servers=os.environ.get("KAFKA_BROKERS", "localhost:9092").split(","),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            for message in consumer:
                record = message.value
                cur.execute(
                    "INSERT INTO {table} (symbol, price, ts) VALUES (%(symbol)s, %(price)s, TO_TIMESTAMP(%(timestamp)s))".format(
                        table=os.environ["SNOW_TABLE"]
                    ),
                    record,
                )
                conn.commit()


if __name__ == "__main__":
    main()
