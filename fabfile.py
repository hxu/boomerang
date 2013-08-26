from __future__ import division
import shutil
from fabric.api import local, cd, env, run, prefix, sudo, execute
from fabric.operations import open_shell, reboot, os, put
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
DEFAULT_AMI = 'ami-0b9ad862'
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
    res = conn.run_instances(ami, key_name=ssh_key, security_groups=[security_group], instance_type=itype,
                             instance_initiated_shutdown_behavior='terminate')
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
    SCRIPT_TEXT += """
fetch_path(key_path='$key_path',
    bucket_name='$bucket_name',
    aws_access_key_id='$aws_access_key_id',
    aws_secret_access_key='$aws_secret_access_key',
    overwrite=1)
"""
    return Template(SCRIPT_TEXT).substitute(key_path=key_path,
                                            bucket_name=bucket_name,
                                            aws_access_key_id=boto.config.get('Credentials', 'aws_access_key_id'),
                                            aws_secret_access_key=boto.config.get('Credentials',
                                                                                  'aws_secret_access_key')
    )


def _generate_run_script(script_name=None, out_path=None):
    """
    Generates the remote script to be run on the instance
    Saves the file to a temporary location and returns the path
    """
    r_log_filename = 'r_log.txt'
    r_log_path = out_path + r_log_filename
    call_command = ['Rscript', '--vanilla', '--verbose', script_name]

    SCRIPT_TEXT = """
# Make sure to make the file first
import os
import subprocess

os.makedirs('$out_path')
outfile = open('$r_log_path', mode='w')
subprocess.call($call_command, stdout=outfile, stderr=subprocess.STDOUT)
outfile.close()

"""

    return Template(SCRIPT_TEXT).substitute(r_log_path=r_log_path, call_command=call_command, out_path=out_path)


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
    SCRIPT_TEXT += """
put_path(path='$path',
    bucket_name='$bucket_name',
    aws_access_key_id='$aws_access_key_id',
    aws_secret_access_key='$aws_secret_access_key',
    overwrite=1)

"""
    return Template(SCRIPT_TEXT).substitute(path=path,
                                            bucket_name=bucket_name,
                                            aws_access_key_id=boto.config.get('Credentials', 'aws_access_key_id'),
                                            aws_secret_access_key=boto.config.get('Credentials',
                                                                                  'aws_secret_access_key')
    )


def generate_script(fetch=False, bucket_name=None, fetch_path=None,
                    put=False, out_path=None,
                    run=False, script_name=None):
    script_text = ''
    if fetch:
        script_text += _generate_fetch_script(fetch_path, bucket_name)

    if run:
        script_text += _generate_run_script(script_name, out_path)

    if put:
        script_text += _generate_put_script(out_path, bucket_name)

    script_text += """
import os
os.system('sudo shutdown -h now')
"""

    return script_text


def send_job(source_script=None, in_directory=None, out_directory=None,
             base_directory='task/',
             load_from_s3=0, s3_bucket_name=None, s3_fetch_path=None,
             put_to_s3=0,
             itype=DEFAULT_INSTANCE_TYPE, ami=DEFAULT_AMI, security_group=DEFAULT_SECURITY_GROUP,
             ssh_key=DEFAULT_SSH_KEY,
             ssh_key_path=DEFAULT_SSH_KEY_PATH):
    """
    Spins up an instance, deploys the job, then exits
    """
    load_from_s3 = int(load_from_s3)
    put_to_s3 = int(put_to_s3)
    out_log_file = base_directory + out_directory + 'shell_log.txt'
    TEMPORARY_FOLDER = '.boom_tmp/'

    # Prepare the local job files
    os.makedirs(TEMPORARY_FOLDER)
    f = open(TEMPORARY_FOLDER + 'boom_task.py', 'w')
    f.write(generate_script(fetch=load_from_s3,
                            bucket_name=s3_bucket_name,
                            fetch_path=s3_fetch_path,
                            put=put_to_s3,
                            out_path=out_directory,
                            run=True,
                            script_name=source_script))
    f.close()

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
        print 'Transferring scripts to instance'
        # TODO: BUG HERE
        put(local_path=TEMPORARY_FOLDER + 'boom_task.py')
        put(local_path=source_script)
        # Kick off the script with tmux
        print 'Kicking off the task'
        run("tmux new-session -s boom_job -d")
        run("tmux pipe-pane -o -t boom_job 'exec cat >> {}'".format(out_log_file))
        run("tmux send -t boom_job python boom_task.py")

    shutil.rmtree(TEMPORARY_FOLDER)


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
