import os
import sys
import imp


DEFAULT_CONFIG_PATH = os.path.expanduser('~/.boomerang')

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

DEFAULT_REGION = 'us-east-1'

DEFAULT_INSTANCE_TYPE = 't1.micro'
DEFAULT_AMI = 'ami-0b9ad862'
DEFAULT_SECURITY_GROUP = ''

DEFAULT_SSH_KEY = ''
DEFAULT_SSH_KEY_PATH = ''

DEFAULT_BUCKET = ''

TEMPORARY_FOLDER = '.boom_tmp/'


# Load the user config and overwrite objects in this module with the values in that file
if os.path.exists(DEFAULT_CONFIG_PATH):
    cfg_file = imp.load_source('user_config', DEFAULT_CONFIG_PATH)
    this_module = sys.modules[__name__]
    for k in dir(cfg_file):
        if k.startswith('_'):
            continue
        else:
            setattr(this_module, k, getattr(cfg_file, k))
