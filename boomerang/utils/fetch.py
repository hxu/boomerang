import boto
import os
import sys
import errno

try:
    from common import _progress_cb, _get_local_path
except ImportError:
    pass

def fetch_path(key_path=None, bucket_name=None, overwrite=0):
    """
    Fetches a path from an S3 bucket
    If the key in the s3 bucket contains slashes, interpret as a file tree and replicate it locally
    """
    prefix = os.getcwd() + '/'
    key_prefix = ''
    overwrite = int(overwrite)
    cb = _progress_cb
    num_cb = 100

    conn = boto.connect_s3()
    b = conn.get_bucket(bucket_name)

    remote_keys = [k for k in b.list(key_path)]
    if len(remote_keys) == 0:
        print 'No files matching path in bucket'
        sys.exit(0)

    for k in remote_keys:
        filename = _get_local_path(k.key, prefix, key_prefix)
        dir = os.path.dirname(filename)

        # try to make the directory
        try:
            os.makedirs(dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        if os.path.exists(filename):
            if not overwrite:
                print 'File {} already exists.  Skipping'.format(filename)
                continue

            print 'File {} already exists.  Overwriting'.format(filename)
        print 'Retrieving {} to {}'.format(k, filename)
        outfile = open(filename, 'w')
        k.get_contents_to_file(outfile, cb=cb, num_cb=num_cb)
