import click

from src.core import test_run


@click.command()
def test():
    test_run()


if __name__ == '__main__':
    test()
