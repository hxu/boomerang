from __future__ import division
from fabric.api import local, cd, env, run, prefix, sudo, execute
from fabric.operations import open_shell, reboot
from fabric.utils import puts, warn
import boto
from itertools import chain
import sys
import time
from string import Template
from boomerang.utils.common import _expand_path
from boomerang.utils.fetch import fetch_path
from boomerang.utils.put import put_path


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


def provision_instance(itype=None, ami=None, security_group=None, ssh_key=None):
    """
    Provisions and instance and returns the instance object
    """
    print "Launching {} instance with ami {}.".format(itype, ami)
    conn = boto.connect_ec2()
    res = conn.run_instances(ami, key_name=ssh_key, security_groups=[security_group], instance_type=itype)
    return res.instances[0]


def _generate_fetch_script(key_path=None, bucket_name=None):
    """
    Portion of the remote script that pulls stuff down from s3
    """
    f = open('boomerang/utils/common.py')
    SCRIPT_TEXT = f.read()
    f.close()
    f = open('boomerang/utils/fetch.py')
    SCRIPT_TEXT += f.read()
    f.close()
    SCRIPT_TEXT +=  """
fetch_path(key_path=$key_path, bucket_name=$bucket_name, overwrite=1)
"""
    return Template(SCRIPT_TEXT).substitute(key_path=key_path, bucket_name=bucket_name)


def _generate_run_script():
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

def _generate_put_script(path=None, bucket_name=None):
    """
    Generates remote script to put files back to s3
    """
    f = open('boomerang/utils/common.py')
    SCRIPT_TEXT = f.read()
    f.close()
    f = open('boomerang/utils/put.py')
    SCRIPT_TEXT += f.read()
    f.close()
    SCRIPT_TEXT +=  """
put_path(path=$path, bucket_name=$bucket_name, overwrite=1)
"""
    return Template(SCRIPT_TEXT).substitute(path=path, bucket_name=bucket_name)


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

    # Kick off the script with tmux
    """
    tmux new-session -s boom_job -d
    tmux pipe-pane -o -t boom_job 'exec cat >> log.txt'
    tmux send -t boom_job python
    """

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
