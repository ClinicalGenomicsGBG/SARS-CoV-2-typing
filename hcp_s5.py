"""
Code for handling the upload of data to the lake from the local S5 torrent machine.
"""

import os
import json
import click
import logging

from NGPinterface.hcp import HCPManager


def connect_hcp(endpoint, access_key, secret_key, bucket='goteborg'):
    hcpm = HCPManager(endpoint, access_key, secret_key)
    hcpm.attach_bucket(bucket)

    return hcpm


def setup_logger(name, log_path=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    stream_handle = logging.StreamHandler()
    stream_handle.setLevel(logging.DEBUG)
    stream_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s'))
    logger.addHandler(stream_handle)

    if log_path:
        file_handle = logging.FileHandler(log_path, 'a')
        file_handle.setLevel(logging.DEBUG)
        file_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s'))
        logger.addHandler(file_handle)

    return logger


@click.group()
def cli():
    pass


@cli.command()
@click.argument('endpoint')
@click.argument('access_key')
@click.argument('secret_key')
@click.option('--logpath')
def wrap(endpoint, access_key, secret_key, logpath):
    """
    TODO
    """
    logger = setup_logger('wrap', logpath)
    target_directory = ''
    run_upload(target_directory, endpoint, access_key, secret_key, logger)


@cli.command()
@click.argument('target_directory')
@click.argument('endpoint')
@click.argument('access_key')
@click.argument('secret_key')
@click.option('--logpath')
def manual(target_directory, endpoint, access_key, secret_key, logpath):
    """
    TODO
    """
    logger = setup_logger('manual', logpath)
    run_upload(target_directory, endpoint, access_key, secret_key, logger)


def run_upload(target_directory, endpont, access_key, secret_key, logger):
    """TODO"""

    # Look for compiled json
    compiled_path = os.path.join(target_directory, 'compiled.json')
    if not os.path.exists(compiled_path):
        logging.warning(f'Could not find expected compiled json at: {compiled_path}')
        return False

    with open(compiled_path, 'r') as inp:
        compiled_data = json.load(inp)


if __name__ == '__main__':
    cli()
