from __future__ import division
import os
import re
import shutil
from itertools import chain
import sys
import time
from string import Template

from fabric.api import env, run
from fabric.api import put as fabput
from fabric.context_managers import cd
from fabric.contrib.files import exists
from fabric.exceptions import NetworkError

import common
import fetch
import put
from common import _expand_path
from fetch import fetch_path
from put import put_path

from boomerang import boom_config
from connection import connect_ec2

__all__ = [
    'common',
    'fetch',
    'put'
]


def provision_instance(itype=None, ami=None, security_group=None, ssh_key=None):
    """
    Provisions and instance and returns the instance object
    """
    print "Launching {} instance with ami {}.".format(itype, ami)
    conn = connect_ec2()
    res = conn.run_instances(ami, key_name=ssh_key, security_groups=[security_group], instance_type=itype,
                             instance_initiated_shutdown_behavior='terminate')
    return res.instances[0]


def _generate_fetch_script(key_path=None, bucket_name=None):
    """
    Portion of the remote script that pulls stuff down from s3
    """
    from templates.remote_fetch import TEMPLATE_TEXT
    return Template(TEMPLATE_TEXT).substitute(key_path=key_path,
                                            bucket_name=bucket_name,
                                            aws_access_key_id=boom_config.AWS_ACCESS_KEY_ID,
                                            aws_secret_access_key=boom_config.AWS_SECRET_ACCESS_KEY
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
    from templates.remote_put import TEMPLATE_TEXT
    return Template(TEMPLATE_TEXT).substitute(path=path,
                                            bucket_name=bucket_name,
                                            aws_access_key_id=boom_config.AWS_ACCESS_KEY_ID,
                                            aws_secret_access_key=boom_config.AWS_SECRET_ACCESS_KEY
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

    # Strip out from __future__ imports and move to the beginning of the file
    imports = set(re.findall('from __future__.+\n', script_text))
    for i in imports:
        script_text = script_text.replace(i, '')
        script_text = i + script_text

    return script_text


def _cleanup_workspace(temp_folder=boom_config.TEMPORARY_FOLDER):
    """
    Cleans up temporary files
    """
    shutil.rmtree(temp_folder)


def _make_workspace(temp_folder=boom_config.TEMPORARY_FOLDER):
    """
    Creates temporary workspace for files
    """
    if os.path.exists(temp_folder) and os.path.isdir(temp_folder):
        shutil.rmtree(temp_folder)
    os.makedirs(temp_folder)


def _expand_local_path():
    pass


def _expand_remote_path():
    pass


def _get_existing_instance(instance_id):
    """
    Gets an existing instance object
    """
    conn = connect_ec2()
    res = [r for r in conn.get_all_instances(instance_id)]
    if len(res) == 0:
        print 'Instance not found. Aborting'
        sys.exit(1)
    elif len(res) > 1:
        print 'Multiple instances found.  Aborting'
        sys.exit(1)
    elif len(res) == 1:
        # We're assuming that each reservation only has one instance
        # Not considering the case where a reservation can have multiple instances
        instance = res[0].instances[0]
    return instance

def send_job(source_script=None, in_directory=None, out_directory=None,
             base_directory='task/',
             load_from_s3=0, s3_bucket_name=None, s3_fetch_path=None,
             put_to_s3=0,
             existing_instance=None,
             itype=None, ami=boom_config.DEFAULT_AMI, security_group=boom_config.DEFAULT_SECURITY_GROUP,
             ssh_key=boom_config.DEFAULT_SSH_KEY,
             ssh_key_path=boom_config.DEFAULT_SSH_KEY_PATH):
    """
    Spins up an instance, deploys the job, then exits
    """
    load_from_s3 = int(load_from_s3)
    put_to_s3 = int(put_to_s3)
    if not out_directory.endswith('/'):
        out_directory += '/'
    out_log_file = base_directory + out_directory + 'shell_log.txt'
    _make_workspace()

    # Prepare the local job files
    f = open(boom_config.TEMPORARY_FOLDER + 'boom_task.py', 'w')
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

    # When provisioning a spot instance
    # res = conn.request_spot_instances(price='0.011', instance_type='t1.micro', image_id='ami-0b9ad862')
    # res[0] gives the spot reservation
    # but this does not have an update method, so need to do
    # conn.get_all_spot_instance_requests(res[0].id)
    # res[0].state = 'active'
    # or res[0].status.code = 'fulfilled'
    # then res[0].instance_id

    try:
        if not existing_instance:
            instance = provision_instance(itype=itype, ami=ami, security_group=security_group, ssh_key=ssh_key)
            print "Waiting for instance to boot"
        else:
            instance = _get_existing_instance(existing_instance)
            print 'Using existing instance {}'.format(existing_instance)
        while instance.state != 'running':
            sys.stdout.write(".")
            time.sleep(5)
            instance.update()
        sys.stdout.write('\n')
    except KeyboardInterrupt:
        print 'Operation cancelled by user.  Attempting to terminate instance'
        if instance:
            # This does not always terminate, if we are really early in the launch process
            instance.terminate()
        _cleanup_workspace()
        sys.exit(1)

    time.sleep(15)
    print "Instance is running at ip {}".format(instance.ip_address)
    print "Connecting as user {}".format(user)

    # Set up the fabric environment to connect to the new machine
    env.host_string = instance.ip_address
    env.user = user
    env.key_filename = ssh_key_path

    attempt = 1
    success = False
    while not success and attempt <= 3:
        try:
            run('uname -a')
            run('pwd')
            success = True
        except NetworkError as e:
            print "Could not connect: {}".format(e)
            print "Retrying"
            attempt += 1
            continue

    if not success:
        print "Could not connect after 3 tries.  Aborting"
        _cleanup_workspace()
        sys.exit(1)

    # Send files to the server
    if exists(base_directory):
        run('rm -R {}'.format(base_directory))
    run('mkdir {}'.format(base_directory))

    fabput(local_path=_expand_path('./' + boom_config.TEMPORARY_FOLDER + 'boom_task.py'), remote_path='~/' + base_directory)
    fabput(local_path=_expand_path('./' + source_script), remote_path='~/' + base_directory)

    with cd(path_to_base_directory):
        print 'Transferring scripts to instance'
        # Kick off the script with tmux
        print 'Kicking off the task'
        run("tmux new-session -s boom_job -d")
        run("tmux pipe-pane -o -t boom_job 'exec cat >> {}'".format(out_log_file))
        run("tmux send -t boom_job 'python boom_task.py' Enter")

    _cleanup_workspace()


def list_instances():
    """
    Lists all instances
    """
    conn = connect_ec2()
    res = conn.get_all_instances()
    if len(res) == 0:
        print('No instances')
    instances = chain.from_iterable([r.instances for r in res])
    for i in instances:
        print('Instance: {}.  Status: {}'.format(i, i.state))


"""
fab send_job:source_script=test.R,in_directory=data/,out_directory=output/,put_to_s3=1,s3_bucket_name=boom_test,load_from_s3=1,s3_fetch_path=data
"""