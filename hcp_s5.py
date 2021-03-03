"""
Code for handling the upload of data to the lake from the local S5 torrent machine.
"""

import click

from NGPinterface.hcp import HCPManager


def connect_hcp(endpoint, access_key, secret_key, bucket='goteborg'):
    hcpm = HCPManager(endpoint, access_key, secret_key)
    hcpm.attach_bucket(bucket)

    return hcpm


@click.group()
def cli():
    pass


@cli.command()
@click.argument('endpoint')
@click.argument('access_key')
@click.argument('secret_key')
def wrap(endpoint, access_key, secret_key):
    """
    TODO
    """
    target_directory = ''
    run_upload(target_directory, endpoint, access_key, secret_key)


@cli.command()
@click.argument('target_directory')
@click.argument('endpoint')
@click.argument('access_key')
@click.argument('secret_key')
def manual(target_directory, endpoint, access_key, secret_key):
    """
    TODO
    """
    run_upload(target_directory, endpoint, access_key, secret_key)


def run_upload(target_directory, endpont, access_key, secret_key):
    """TODO"""
    pass


if __name__ == '__main__':
    cli()
