TEMPLATE_TEXT = """
from pprint import pprint
import boto
import os
import sys

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


def _progress_cb(completed, total):
    if total != 0:
        sys.stdout.write('\\rTransferred {} of {} ({:.2%})'.format(completed, total, float(completed) / total))
        sys.stdout.flush()
    if completed == total:
        sys.stdout.write(' - DONE')
        sys.stdout.flush()
        sys.stdout.write('\\n')


def _expand_path(path):
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    return os.path.abspath(path)


def _get_key_name(fullpath, prefix, key_prefix):
    if fullpath.startswith(prefix):
        key_name = fullpath[len(prefix):]
    else:
        key_name = fullpath
    l = key_name.split(os.sep)
    return key_prefix + '/'.join(l)


def _get_local_path(key_name, prefix, key_prefix):
    if not prefix.endswith(os.sep):
        prefix += os.sep

    if key_name.startswith(key_prefix):
        full_path = key_name[len(key_prefix):]
    else:
        full_path = key_name
    l = full_path.split('/')
    return prefix + os.sep.join(l)


def _upload_part(bucketname, aws_key, aws_secret, multipart_id, part_num,
                 source_path, offset, bytes, debug, cb, num_cb,
                 amount_of_retries=10):
    if debug == 1:
        print "_upload_part(%s, %s, %s)" % (source_path, offset, bytes)

    def _upload(retries_left=amount_of_retries):
        try:
            if debug == 1:
                print 'Start uploading part #%d ...' % part_num
            conn = boto.connect_s3(aws_key, aws_secret)
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
    conn = boto.connect_s3(aws_key, aws_secret)
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
    k = bucket.new_key(key_name)
    k.set_contents_from_filename(fullpath, *kargs, **kwargs)


def put_path(path=None, bucket_name=None, overwrite=0,
             aws_access_key_id=None, aws_secret_access_key=None):
    if bucket_name is None:
        print 'You must provide a bucket name'
        sys.exit(0)

    cb = _progress_cb
    num_cb = 100
    debug = 0
    reduced = True
    grant = None
    headers = {}

    overwrite = int(overwrite)
    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
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


put_path(path='$path',
         bucket_name='$bucket_name',
         aws_access_key_id='$aws_access_key_id',
         aws_secret_access_key='$aws_secret_access_key',
         overwrite=1)
"""
