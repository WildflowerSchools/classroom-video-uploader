import click

from uploader.cleanup.janitor import cleanup_active, queue_missed


@click.command()
@click.option('--action', required=True, type=click.Choice(['cleanup-active', 'queue-missed']))
def main(action):
    if action == "cleanup-active":
        cleanup_active()
    elif action == "queue-missed":
        queue_missed()


if __name__ == '__main__':
    main()  # pylint: disable=E1120
