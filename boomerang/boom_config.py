import os
import ConfigParser


DEFAULT_CONFIG_PATH = os.path.expanduser('~/.boomerang')

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''


if os.path.exists(DEFAULT_CONFIG_PATH):
    cfg_file = ConfigParser.SafeConfigParser()
    cfg_file.read(DEFAULT_CONFIG_PATH)
    if cfg_file.has_option('aws_credentials', 'aws_access_key_id'):
        AWS_ACCESS_KEY_ID = cfg_file.get('aws_credentials', 'aws_access_key_id')
    if cfg_file.has_option('aws_credentials', 'aws_secret_access_key'):
        AWS_SECRET_ACCESS_KEY = cfg_file.get('aws_credentials', 'aws_secret_access_key')
