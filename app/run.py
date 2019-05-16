from dotenv import load_dotenv
import os
from s3 import S3
import lib.file_operator as fo
from lib.log import setup_logger
from lib.notification import SlackNotification
from config import Config
from control_data import ControlData
import json
from itertools import chain
from functools import partial
from athena import AthenaClient

load_dotenv()
logger = setup_logger(__name__)
slackBot = SlackNotification(__name__)


def main():
    print(" [Athena Runner Step 1/5] read config file... ")
    data = Config("../configs/prod.json").data
    s3 = S3(bucket=data['controlBucket'])

    print(" [Athena Runner Step 2/5] read control file... ")
    control_dict = read_control(s3, data["controlKey"])

    print(" [Athena Runner Step 3/5] append control file... ")
    controlData = ControlData(control_dict, data)

    # print(controlData)
    # write_control(str(controlData))

    # filter the controlData to get all hour job that has state not equal to SUCCEEDED
    day_hour_tuple_list_of_list = list(map(lambda day: zip(
        [f"{day['year']}-{day['month']}-{day['day']}"]*len(day["hourlist"]), day["hourlist"]), controlData.date_list))
    day_hour_tuple_list_flaterned = list(chain.from_iterable(
        day_hour_tuple_list_of_list))  # [ day_hour_tuple for sublist in day_hour_tuple_list_of_list for day_hour_tuple in sublist]
    # print(json.dumps(day_hour_tuple_list_flaterned, indent=4))
    hour_jobs_to_process = list(filter(lambda x: x[1]["state"] != "SUCCEEDED",
                                       day_hour_tuple_list_flaterned))


# {
#     "database": "default",
#     "resultsLocation": "s3://athena-runner-ap-southeast-2-test/product-video-audience/results/",
#     "encryptQueryResults": "false",
#     "encryptionType": "",
#     "encryptionKey": "",
#     "controlBucket": "athena-runner-ap-southeast-2-test",
#     "controlKey": "product-video-audience/control.json",
#     "queryBucket": "athena-runner-ap-southeast-2",
#     "queryKey": "product-video-audience/query.sql",
#     "maxQueries": "4",
#     "controlDays": "-1",
#     "appendHours": "false",
#     "timeoutMinutes": "55",
#     "sleepSeconds": "10",
#     "dropTableName": "",
#     "parquet"
# }
# region = 'ap-southeast-2', db = 'default', max_queries = 3, max_retries = 3, timeout_minutes = 10,
#                  sleep_seconds = 10, s3_parquet = None

    # create athena client with config
    athena = AthenaClient(db=data['database'], max_queries=data['maxQueries'],
                          timeout_minutes=data['timeoutMinutes'], sleep_seconds=data['sleepSeconds'], s3_parquet=data['parquet'])

    add_task_with_client = partial(add_task, athena)
    add_task_with_client_config = partial(add_task_with_client, data)

    list(map(add_task_with_client_config, hour_jobs_to_process))

    athena.wait_for_completion()

    # write_control(json.dumps(hour_jobs_to_process, indent=4))


def add_task(athena, data, hour_job):
    name = data['controlKey'].split("/")[0]
    date_string = hour_job[0]
    control_hour_job = hour_job[1]
    hour_string = str(control_hour_job["hour"])

    sql = get_query_from_s3(data['queryBucket'], data['queryKey'])

    sql = sql.replace("<date>", date_string).replace("<hour>", hour_string)

    output_location = f"""{data['resultsLocation']}/{date_string.split("-")[0]}/{date_string.split("-")[1]}/{date_string.split("-")[2]}"""

    athena.add_query(sql, name, output_location,
                     data["encryptQueryResults"], data["encryptionType"], data["encryptionKey"], data["dropTableName"], data["parquet"])


def pretty_print(d):
    print(json.dumps(d, indent=4))


def get_query_from_s3(queryBucket, queryKey):
    s3 = S3(bucket=queryBucket)
    s3.get(f"s3://{ queryBucket}/{queryKey}", "query.txt")
    with open("query.txt") as f:
        query = f.read()
    return query


def read_control(s3, key):
    # s3.get(key, "control.json")
    if os.path.isfile("control.json"):
        with open("control.json") as f:
            controlData = json.load(f)
        return controlData
    return None


def write_control(controlData):
    with open("control_output.json", "w") as f:
        f.write(controlData)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception("main() fail: " + str(e))
        # slackBot.warn("main() fail: " + str(e))
