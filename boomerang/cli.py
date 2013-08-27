from argparse import ArgumentParser


class BoomFetchCLI(ArgumentParser):
    def __init__(self):
        super(BoomFetchCLI, self).__init__(
            description="Fetch objects from S3"
        )

        self.add_argument(
            'bucket_name',
            help="The remote bucket name to connect to"
        )

        self.add_argument(
            'key_path',
            help="The key to retrieve.  If it is folder, will retrieve the whole tree"
        )

        self.add_argument(
            '-o --overwrite',
            dest='overwrite',
            action='store_true',
            help="Overwrite local files"
        )
