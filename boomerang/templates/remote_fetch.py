TEMPLATE_TEXT = """
import boto
import os
import sys
import errno

def _progress_cb(completed, total):
    if total != 0:
        sys.stdout.write('\\rTransferred {} of {} ({:.2%})'.format(completed, total, float(completed) / total))
        sys.stdout.flush()
    if completed == total:
        sys.stdout.write(' - DONE')
        sys.stdout.flush()
        sys.stdout.write('\\n')

def _get_local_path(key_name, prefix, key_prefix):
    if not prefix.endswith(os.sep):
        prefix += os.sep

    if key_name.startswith(key_prefix):
        full_path = key_name[len(key_prefix):]
    else:
        full_path = key_name
    l = full_path.split('/')
    return prefix + os.sep.join(l)

def fetch_path(key_path=None, bucket_name=None, overwrite=0,
               aws_access_key_id=None, aws_secret_access_key=None):
    prefix = os.getcwd() + '/'
    key_prefix = ''
    overwrite = int(overwrite)
    cb = _progress_cb
    num_cb = 100

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    b = conn.get_bucket(bucket_name)

    remote_keys = [k for k in b.list(key_path)]
    if len(remote_keys) == 0:
        print 'No files matching path in bucket'
        sys.exit(0)

    for k in remote_keys:
        filename = _get_local_path(k.key, prefix, key_prefix)
        filedir = os.path.dirname(filename)

        # try to make the directory
        try:
            os.makedirs(filedir)
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


fetch_path(key_path='$key_path',
           bucket_name='$bucket_name',
           aws_access_key_id='$aws_access_key_id',
           aws_secret_access_key='$aws_secret_access_key',
           overwrite=1)
"""