import time

import pulsar

from consumer_example import (
    ENV_PATH,
    create_pulsar_client,
    load_config,
    mask_value,
    subscription_name,
    subscription_topic,
    validate_config,
)


def main() -> int:
    config = load_config()
    missing = validate_config(config, require_pulsar_url=True, require_influx=False)
    if missing:
        print("CONFIG FAILED")
        for item in missing:
            print(f"- {item}")
        return 2

    print("CONFIG OK")
    print(f".env path: {ENV_PATH}")
    print(f"Access ID: {mask_value(config.access_id)}")
    print(f"Access Key loaded: {bool(config.access_key)}")
    print(f"Access Key length: {len(config.access_key) if config.access_key else 0}")
    print(f"Endpoint: {config.pulsar_server_url}")
    print(f"MQ env: {config.mq_env}")
    print(f"Topic: {mask_value(config.access_id)}/out/{config.mq_env}")
    print(f"Subscription: {mask_value(subscription_name(config))}")

    client = None
    consumer = None
    try:
        print("AUTH CREATED")
        client = create_pulsar_client(config)
        print("CLIENT CREATED")
        print("SUBSCRIBING")
        consumer = client.subscribe(
            subscription_topic(config),
            subscription_name(config),
            consumer_type=pulsar.ConsumerType.Failover,
        )
        print("SUBSCRIBE SUCCESS")
        time.sleep(10)
        return 0
    finally:
        if consumer is not None:
            consumer.close()
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
