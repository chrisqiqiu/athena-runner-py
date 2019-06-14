# from dotenv import load_dotenv
import os
from s3 import S3

from lib.log import setup_logger
from lib.notification import SlackNotification
from config import Config
from control_data import ControlData
import json
from itertools import chain
from functools import partial
from athena import AthenaClient

# load_dotenv()
logger = setup_logger(__name__)
slackBot = SlackNotification(__name__)


def main():
    logger.info("Read config file for task list... ")
    # os.environ['CONTROLCONFIGPATH'] export "../configs/prod.json" to os.environ['CONTROLCONFIGPATH']
    data = Config(os.environ['CONTROLCONFIGPATH']).data

    list(map(process_each_config, data['steps']))


def process_each_config(data):

    logger.info(" [Athena Runner Step 1/5] read config file... ")

    control_s3 = S3(bucket=data['controlBucket'])

    logger.info(" [Athena Runner Step 2/5] read control file... ")
    control_dict = read_control(control_s3, data["controlKey"])

    logger.info(" [Athena Runner Step 3/5] append control file... ")
    control_data = ControlData(control_dict, data)

    # filter the controlData to get all hour job that has state not equal to SUCCEEDED
    day_hour_tuple_list_of_list = list(map(lambda day: zip(
        [f"{day['year']}-{day['month']}-{day['day']}"]*len(day["hourlist"]), day["hourlist"]), control_data.date_list))
    day_hour_tuple_list_flaterned = list(chain.from_iterable(
        day_hour_tuple_list_of_list))  # [ day_hour_tuple for sublist in day_hour_tuple_list_of_list for day_hour_tuple in sublist]

    hour_jobs_to_process = list(filter(lambda x: x[1]["state"] != "SUCCEEDED",
                                       day_hour_tuple_list_flaterned))

    # create athena client with config
    athena = AthenaClient(db=data['database'], max_queries=data['maxQueries'],
                          timeout_minutes=data['timeoutMinutes'], sleep_seconds=data['sleepSeconds'], workgroup=data['workgroup'], control_s3=control_s3, control_key=data['controlKey'], parquet=data.get("parquet"), control_data=control_data)

    add_query_with_config = partial(athena.add_query, data)

    sql = get_query_from_s3(data['queryBucket'], data['queryKey'])

    add_query_with_config_and_sql = partial(add_query_with_config, sql)

    list(map(add_query_with_config_and_sql, hour_jobs_to_process))

    athena.wait_for_completion()


def get_query_from_s3(queryBucket, queryKey):
    query_s3 = S3(bucket=queryBucket)
    query_s3.get(queryKey, "query.sql")
    with open("query.sql") as f:
        query = f.read()
    return query


def pretty_print(d):
    print(json.dumps(d, indent=4))


def read_control(s3, key):
    s3.get(key, "control.json")
    if os.path.isfile("control.json"):
        with open("control.json") as f:
            controlData = json.load(f)
        return controlData
    return None


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception("main() fail: " + str(e))
        slackBot.warn("main() fail: " + str(e))
