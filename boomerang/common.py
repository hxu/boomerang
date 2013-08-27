from __future__ import division
import os
import sys

def _progress_cb(completed, total):
    if total != 0:
        sys.stdout.write('\rTransferred {} of {} ({:.2%})'.format(completed, total, completed / total))
        sys.stdout.flush()
    if completed == total:
        sys.stdout.write(' - DONE')
        sys.stdout.flush()
        sys.stdout.write('\n')


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
