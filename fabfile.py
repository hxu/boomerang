from pprint import pprint
from fabric.api import local, cd, env, run, prefix, sudo, execute
from fabric.operations import open_shell, reboot
from fabric.utils import puts, warn
import boto
from itertools import chain
import os
import sys
import time
import errno
from boto.utils import fetch_file


try:
    # multipart portions copyright Fabian Topfstedt
    # https://gist.github.com/924094

    import math
    import mimetypes
    from multiprocessing import Pool
    from boto.s3.connection import S3Connection
    from filechunkio import FileChunkIO


    multipart_capable = True
except ImportError as err:
    multipart_capable = False

DEFAULT_INSTANCE_TYPE = 't1.micro'
DEFAULT_AMI = 'ami-c30360aa'
DEFAULT_REGION = 'us-east-1'
DEFAULT_BUCKET = 'boom_test'
DEFAULT_SSH_KEY = 'hgcrpd'
DEFAULT_SSH_KEY_PATH = '~/aws/hgcrpd.pem'
DEFAULT_SECURITY_GROUP = 'ssh-only'

"""
You should have a .boto file in your home directory for the Boto config
Also need to have Fabric installed
"""

"""
From boto/bin/s3put/
"""


def _upload_part(bucketname, aws_key, aws_secret, multipart_id, part_num,
                 source_path, offset, bytes, debug, cb, num_cb,
                 amount_of_retries=10):
    """
    Uploads a part with retries.
    """
    if debug == 1:
        print "_upload_part(%s, %s, %s)" % (source_path, offset, bytes)

    def _upload(retries_left=amount_of_retries):
        try:
            if debug == 1:
                print 'Start uploading part #%d ...' % part_num
            conn = S3Connection(aws_key, aws_secret)
            conn.debug = debug
            bucket = conn.get_bucket(bucketname)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset,
                                     bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num,
                                                 cb=cb, num_cb=num_cb)
                    break
        except Exception, exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                print 'Failed uploading part #%d' % part_num
                raise exc
        else:
            if debug == 1:
                print '... Uploaded part #%d' % part_num

    _upload()


def _multipart_upload(bucketname, aws_key, aws_secret, source_path, keyname,
                      reduced, debug, cb, num_cb, acl='private', headers={},
                      guess_mimetype=True, parallel_processes=4):
    """
    Parallel multipart upload.
    """
    conn = S3Connection(aws_key, aws_secret)
    conn.debug = debug
    bucket = conn.get_bucket(bucketname)

    if guess_mimetype:
        mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
        headers.update({'Content-Type': mtype})

    mp = bucket.initiate_multipart_upload(keyname, headers=headers,
                                          reduced_redundancy=reduced)

    source_size = os.stat(source_path).st_size
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                          5242880)
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

    pool = Pool(processes=parallel_processes)
    for i in range(chunk_amount):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1
        pool.apply_async(_upload_part, [bucketname, aws_key, aws_secret, mp.id,
                                        part_num, source_path, offset, bytes,
                                        debug, cb, num_cb])
    pool.close()
    pool.join()

    if len(mp.get_all_parts()) == chunk_amount:
        mp.complete_upload()
        key = bucket.get_key(keyname)
        key.set_acl(acl)
    else:
        mp.cancel_upload()


def _singlepart_upload(bucket, key_name, fullpath, *kargs, **kwargs):
    """
    Single upload.
    """
    k = bucket.new_key(key_name)
    k.set_contents_from_filename(fullpath, *kargs, **kwargs)


def _expand_path(path):
    """
    Expands paths to full paths
    """
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    return os.path.abspath(path)


def _get_key_name(fullpath, prefix, key_prefix):
    """
    Takes a file path and strips out the prefix while adding in key_prefix
    """
    if fullpath.startswith(prefix):
        key_name = fullpath[len(prefix):]
    else:
        key_name = fullpath
    l = key_name.split(os.sep)
    return key_prefix + '/'.join(l)


def _get_local_path(key_name, prefix, key_prefix):
    """
    Opposite of _get_key_name
    From a remote key, get the full local path of the file
    """
    if not prefix.endswith(os.sep):
        prefix += os.sep

    if key_name.startswith(key_prefix):
        full_path = key_name[len(key_prefix):]
    else:
        full_path = key_name
    l = full_path.split('/')
    return prefix + os.sep.join(l)


def put_path(path=None, bucket_name=None, overwrite=0):
    """
    Puts a path to S3
    If the path is a file, puts just the file into the bucket
    If the path is a folder, recursively puts the folder into the bucket
    """
    if bucket_name is None:
        print 'You must provide a bucket name'
        sys.exit(0)

    cb = None
    num_cb = 0
    debug = 0
    reduced = True
    grant = None
    headers = {}
    aws_access_key_id = None
    aws_secret_access_key = None

    overwrite = int(overwrite)
    conn = boto.connect_s3()
    b = conn.get_bucket(bucket_name)
    path = _expand_path(path)
    files_to_check_for_upload = []
    existing_keys_to_check_against = []
    prefix = os.getcwd() + '/'
    key_prefix = ''

    # Take inventory of the files to upload
    # For directories, walk recursively
    files_in_bucket = [k.name for k in b.list()]
    if os.path.isdir(path):
        print 'Getting list of existing keys to check against'
        for root, dirs, files in os.walk(path):
            for p in files:
                if p.startswith("."):
                    continue
                full_path = os.path.join(root, p)
                key_name = _get_key_name(full_path, prefix, key_prefix)
                files_to_check_for_upload.append(full_path)
                if key_name in files_in_bucket:
                    existing_keys_to_check_against.append(full_path)
    # for single files, just add the file
    elif os.path.isfile(path):
        full_path = os.path.abspath(path)
        key_name = _get_key_name(full_path, prefix, key_prefix)
        files_to_check_for_upload.append(full_path)
        if key_name in files_in_bucket:
            existing_keys_to_check_against.append(full_path)
    # we are trying to upload something unknown
    else:
        print "I don't know what %s is, so i can't upload it" % path

    print "{} files to upload:".format(len(files_to_check_for_upload))
    pprint(files_to_check_for_upload)
    print "{} Existing files already in bucket:".format(len(existing_keys_to_check_against))
    pprint(existing_keys_to_check_against)

    for full_path in files_to_check_for_upload:
        key_name = _get_key_name(full_path, prefix, key_prefix)

        if full_path in existing_keys_to_check_against:
            if not overwrite and b.get_key(key_name):
                print 'Skipping %s as it exists in s3' % full_path
                continue

        print 'Copying %s to %s/%s' % (full_path, bucket_name, key_name)

        # 0-byte files don't work and also don't need multipart upload
        if os.stat(full_path).st_size != 0 and multipart_capable:
            _multipart_upload(bucket_name, aws_access_key_id,
                              aws_secret_access_key, full_path, key_name,
                              reduced, debug, cb, num_cb,
                              grant or 'private', headers)
        else:
            _singlepart_upload(b, key_name, full_path, cb=cb, num_cb=num_cb,
                               policy=grant, reduced_redundancy=reduced,
                               headers=headers)


def fetch_path(key_path=None, bucket_name=None, overwrite=0):
    """
    Fetches a path from an S3 bucket
    If the key in the s3 bucket contains slashes, interpret as a file tree and replicate it locally
    """
    prefix = os.getcwd() + '/'
    key_prefix = ''
    overwrite = int(overwrite)

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
        k.get_contents_to_file(outfile)


def provision_instance(itype=None, ami=None, security_group=None, ssh_key=None):
    """
    Provisions and instance and returns the instance object
    """
    print "Launching {} instance with ami {}.".format(itype, ami)
    conn = boto.connect_ec2()
    res = conn.run_instances(ami, key_name=ssh_key, security_groups=[security_group], instance_type=itype)
    return res.instances[0]


def generate_script():
    """
    Generates the remote script to be run on the instance
    Saves the file to a temporary location and returns the path
    """
    SCRIPT_TEXT = """
    # Make sure to make the file first
    outfile = open('./data/outfile.Rout', mode='w')
    subprocess.call('Rscript --vanilla --verbose test.R'.split(' '), stdout=outfile, stderr=subprocess.STDOUT)
    outfile.close()
    """


def send_job(source_script=None, in_directory=None, out_directory=None,
             base_directory='task/',
             itype=DEFAULT_INSTANCE_TYPE, ami=DEFAULT_AMI, security_group=DEFAULT_SECURITY_GROUP,
             ssh_key=DEFAULT_SSH_KEY,
             ssh_key_path=DEFAULT_SSH_KEY_PATH):
    """
    Spins up an instance, deploys the job, then exits
    """
    user = 'ubuntu'
    ssh_key_path = _expand_path(ssh_key_path)
    path_to_base_directory = '~/{}'.format(base_directory)

    instance = None
    try:
        instance = provision_instance(itype=itype, ami=ami, security_group=security_group, ssh_key=ssh_key)
        print "Waiting for instance to boot"
        while instance.state != 'running':
            print "."
            time.sleep(5)
            instance.update()
    except KeyboardInterrupt:
        print 'Operation cancelled by user.  Attempting to terminate instance'
        if instance:
            instance.terminate()
        sys.exit(1)

    print "Instance is running at ip {}".format(instance.ip_address)
    print "Connecting as user {}".format(user)

    # Set up the fabric environment to connect to the new machine
    env.host_string = instance.ip_address
    env.user = user
    env.key_filename = ssh_key_path

    time.sleep(15)
    run('uname -a')
    run('ls -la')
    run('pwd')

    # Send files to the server
    with cd('~'):
        run('mkdir {}'.format(base_directory))
    with cd(path_to_base_directory):
        pass

    instance.terminate()


def list_instances():
    """
    Lists all instances
    """
    conn = boto.connect_ec2()
    res = conn.get_all_instances()
    if len(res) == 0:
        print('No instances')
    instances = chain.from_iterable([r.instances for r in res])
    for i in instances:
        print('Instance: {}.  Status: {}'.format(i, i.state))
