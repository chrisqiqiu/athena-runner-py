
import re
import gzip
import shutil


def remove_prefix(key):
    object_dest = '%s' % re.search(r'^(.*\/)?(.*)', key).group(2)
    return object_dest


def gzip_file(src_path):
    dst_path = src_path+".gz"
    with open(src_path, 'rb') as src, gzip.open(dst_path, 'wb') as dst:
        shutil.copyfileobj(src, dst)
    return dst_path


def get_key(prefix, path):
    return path.replace(prefix+"/", "")
