import os
import click


class IncompleteTransferError(Exception):
    """Raise on attempting work on incomplete run transfer."""


class RunContext:
    def __init__(self, input_path):
        self.input_path = input_path
        self.input_name = os.path.basename(self.input_path)

    @property
    def has_finished_transfer(self):
        """Return bool of completion signal file for transfer completion existing."""
        completion_signal = 'metadata.json'
        return os.path.exists(os.path.join(self.input_path, completion_signal))


@click.command()
@click.argument('input_directory_path', type=click.Path(exists=True))
def main(input_directory_path):

    ctx = RunContext(input_directory_path)

    if not ctx.has_finished_transfer:
        raise IncompleteTransferError(f'Could not find completion signal for {ctx.input_path}')


if __name__ == '__main__':
    main()
