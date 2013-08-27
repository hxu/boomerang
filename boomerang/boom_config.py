import os
import sys
import imp


DEFAULT_CONFIG_PATH = os.path.expanduser('~/.boomerang')

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

if os.path.exists(DEFAULT_CONFIG_PATH):
    cfg_file = imp.load_source('user_config', DEFAULT_CONFIG_PATH)
    this_module = sys.modules[__name__]
    for k in dir(cfg_file):
        if k.startswith('_'):
            continue
        else:
            setattr(this_module, k, getattr(cfg_file, k))
