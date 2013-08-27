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


class BoomPutCLI(ArgumentParser):
    def __init__(self):
        super(BoomPutCLI, self).__init__(
            description="Put objects to S3"
        )

        self.add_argument(
            'bucket_name',
            help="The remote bucket name to connect to"
        )
        self.add_argument(
            'path',
            help="The local path to send.  If it is a file, sends just the file.  If it is a path, sends the whole tree"
        )
        self.add_argument(
            '-o --overwrite',
            dest='overwrite',
            default=False,
            help="Overwrite remote files"
        )


class BoomCLI(ArgumentParser):
    def __init__(self):
        super(BoomCLI, self).__init__(
            description="Launch a Boomerang job."
        )

        self.add_argument(
            'script',
            help="The R script to run.  Should be self-contained (not requiring any other external files."
        )

        self.add_argument(
            'out_path',
            help="The folder to save logs and output to"
        )

        self.add_argument(
            '-b --bucket',
            dest='bucket_name',
            help='S3 bucket to fetch/put files to.'
        )

        self.add_argument(
            '-p --put',
            dest='put_results',
            action='store_true',
            help='Put the output folder to S3 after the script is done running.'
        )

        self.add_argument(
            '-f --fetch',
            dest='fetch_path',
            help='The remote key to get from S3 before kicking off the script'
        )

        self.add_argument(
            '-i --instance',
            dest='instance',
            help="A running instance name.  If provided, will use the existing instance instead of spinning up a new one"
        )

        self.add_argument(
            '-d --dependencies',
            dest='deps',
            nargs='+',
            # Sometimes dependencies may be in other directories, in which case we'd have to handle putting those to the server in the right path
            help="Dependencies of the script.  NOT IMPLEMENTED"
        )

    def validate(self):
        # Bucket must be provided if flag -p or -f is pprovided
        pass
