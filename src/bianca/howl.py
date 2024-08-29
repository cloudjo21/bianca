import click
import json
import sys
import time

from typing import  Optional

from tunip.log_pubsub import LogSubscriber
from tunip.log_trx import LogReceiver
from tunip.service_config import get_service_config
from tunip.webhook_mappings import WebHookMappings

from bianca import LOGGER


@click.command()
@click.option(
    "--project",
    type=click.STRING,
    required=True,
    help="project_id for gcp"
)
@click.option(
    "--topic",
    type=click.STRING,
    required=True,
    help="topic_id to subscription of gcp-pubsub"
)
@click.option(
    "--subscription",
    type=click.STRING,
    required=True,
    help="subscription_id to corresponding to subscription of topic of gcp-pubsub"
)
@click.option(
    "--webhook_mapping_filepath",
    type=click.STRING,
    required=True,
    help="json file path mapping from channel to webhook URL of slack notification"
)
@click.option(
    "--verbose_period_seconds",
    type=click.INT,
    required=False,
    default=60,
    help="json file path mapping from channel to webhook URL of slack notification"
)
def main(project, topic, subscription, webhook_mapping_filepath, verbose_period_seconds):

    service_config = get_service_config()

    LOGGER.info(f"==== START {__file__} ====")
    LOGGER.info(f"SERVICE CONFIGURATION ====>\t\n{service_config.config.dict}")

    webhook_mappings: Optional[WebHookMappings] = None
    with open(webhook_mapping_filepath, 'r') as f:
        webhook_mappings = WebHookMappings.model_validate(json.load(f))

    assert webhook_mappings is not None
    webhook_mapping = dict([(wm.channel, wm.webhook_url) for wm in webhook_mappings.webhook_mapping])

    log_subscriber = LogSubscriber(service_config, project_id=project, topic_id=topic, subscription_id=subscription, logger=LOGGER)
    log_receiver = LogReceiver(
        name="error_notifier",
        subscriber=log_subscriber,
        webhook_mapping=webhook_mapping,
        timeout=3
    )

    channels = list(webhook_mapping.keys())
    n_channels = len(channels)

    tick = 0
    while True:
        tick_channel_unit = tick % n_channels

        log_receiver(channel=channels[tick_channel_unit])

        if (tick % verbose_period_seconds) > (verbose_period_seconds - (n_channels+1)):
            LOGGER.info(f"receive log from {channels[tick_channel_unit]}")
        time.sleep(1)

        tick += 1
        if tick == sys.maxsize:
            tick = 0


if __name__ == "__main__":
    main()
