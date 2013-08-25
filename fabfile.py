from pprint import pprint
from fabric.api import local, cd, env, run, prefix, sudo, execute
from fabric.operations import open_shell, reboot
from fabric.utils import puts, warn
import boto
from itertools import chain

DEFAULT_INSTANCE_TYPE = ''
DEFAULT_AMI = ''
DEFAULT_REGION = 'us-east-1'
DEFAULT_BUCKET = 'cyberport-ams-solar'

"""
You should have a .boto file in your home directory for the Boto config
Also need to have Fabric installed
"""

def provision_instance(itype=None, ami=None, security_group=None, ssh_key=None):
    """
    Provisions and instance and returns the instance object
    """
    conn = boto.ec2.connect_to_region(DEFAULT_REGION)
    res = conn.run_instances(ami, key_name=ssh_key, security_groups=[security_group], instance_type=itype)
    return res.instances[0]


def expand_path(path):
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    return os.path.abspath(path)


def get_key_name(fullpath, prefix, key_prefix):
    if fullpath.startswith(prefix):
        key_name = fullpath[len(prefix):]
    else:
        key_name = fullpath
    l = key_name.split(os.sep)
    return key_prefix + '/'.join(l)


def put_path(path=None, bucket=None):
    """
    Puts a path to S3
    If the path is a file, puts just the file into the bucket
    If the path is a folder, recursively puts the folder into the bucket
    """
    conn = boto.connect_s3()
    b = c.get_bucket(bucket)
    path = expand_path(path)
    files_to_check_for_upload = []
    existing_keys_to_check_against = []
    prefix = os.getcwd() + '/'
    key_prefix = ''

    # Take inventory of the files to upload
    # upload a directory of files recursively
    if os.path.isdir(path):
        print 'Getting list of existing keys to check against'
            for key in b.list(get_key_name(path, prefix, key_prefix)):
                existing_keys_to_check_against.append(key.name)
        for root, dirs, files in os.walk(path):
            for path in files:
                if path.startswith("."):
                    continue
                files_to_check_for_upload.append(os.path.join(root, path))
    # upload a single file
    elif os.path.isfile(path):
        fullpath = os.path.abspath(path)
        key_name = get_key_name(fullpath, prefix, key_prefix)
        files_to_check_for_upload.append(fullpath)
        existing_keys_to_check_against.append(key_name)
    # we are trying to upload something unknown
    else:
        print "I don't know what %s is, so i can't upload it" % path


def generate_script():
    """
    Generates the remote script to be run on the instance
    Saves the file to a temporary location and returns the path
    """
    pass

def list_instances():
    """
    Lists all instances
    """
    conn = boto.ec2.connect_to_region(DEFAULT_REGION)
    res = conn.get_all_instances()
    if len(res) == 0:
        print('No instances')
    instances = chain.from_iterable([r.instances for r in res])
    for i in instances:
        print('Instance: {}.  Status: {}'.format(i, i.state))
