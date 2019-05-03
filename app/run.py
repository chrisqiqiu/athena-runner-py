from dotenv import load_dotenv
import os
from s3 import S3
import lib.file_operator as fo
from lib.log import setup_logger
from lib.notification import SlackNotification
from config import Config
from appnexus_api import AppnexusApi
from functools import partial


load_dotenv()
logger = setup_logger(__name__)
slackBot = SlackNotification(__name__)


def main():
    data = loadConfigData()
    src_s3 = S3(bucket=data['src_s3_bucket'],
                prefix=data['src_s3_prefix'],
                access_key=os.environ.get("aws_access_key"),
                secret_access=os.environ.get("aws_secret_access")
                )
    paths, files = getPathsFromS3AndLocal(src_s3, data)

    auth = {
        "auth": {
            "username": os.environ['appnexus_username'],
            "password": os.environ['appnexus_password']
        }
    }

    appnexApiClient = AppnexusApi(auth)

    processEachFileWithS3 = partial(processEachFile, src_s3)
    processEachFileWithS3AndAPIClient = partial(
        processEachFileWithS3, appnexApiClient)

    job_ids = list(map(processEachFileWithS3AndAPIClient, paths, files))

    list(map(appnexApiClient.CheckStatus, job_ids))

    if data.get('archive'):
        logger.info(f"Moving files { str(files)} to archive ")
        list(map(src_s3.move_to_archive, paths))


def getFilesChunksToUpload(files, chunkSize):
    return [files[i:i+chunkSize]
            for i in range(0, len(files), chunkSize)]


def getPathsFromS3AndLocal(src_s3, data):

    get_key_with_prefix = partial(fo.get_key, data['src_s3_prefix'])

    paths = list(map(get_key_with_prefix, src_s3.list_objects()))

    files = list(map(fo.remove_prefix, paths))

    return paths, files


def processEachFile(src_s3, appnexApiClient, path, file):
    path = src_s3.get(path, file)
    logger.info(f"File downloaded is {file}")
    gzipfile = fo.gzip_file(file)
    logger.info(f"File zipped is {gzipfile}")
    os.remove(file)
    logger.info(f"Removed file {file}")
    job_id = appnexApiClient.uploadFile(gzipfile)
    logger.info(f"Uploaded gzip file {gzipfile}. Job id is {job_id}")
    os.remove(gzipfile)
    logger.info(f"Removed gzip file {gzipfile}")
    return job_id


def loadConfigData():
    return Config("../config/prod.json").data


if __name__ == '__main__':
    try:
        main()
        import sys
        sys.stdout.flush()
    except Exception as e:
        logger.exception("main() fail: " + str(e))
        slackBot.warn("main() fail: " + str(e))
