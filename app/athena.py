import boto3
import logging
import time
import os
import re
from task_queue import TaskQueue, RetryException
from s3 import S3
from awsretry import AWSRetry

from lib.log import setup_logger
from lib.notification import SlackNotification

logger = setup_logger(__name__)


class AthenaClientError(Exception):
    """
    A generic class for reporting errors in the athena client
    """

    def __init__(self, reason):
        Exception.__init__(
            self, 'Athena Client failed: reason {}'.format(reason))
        self.reason = reason


class AthenaClient(TaskQueue):
    """
    A client for AWS Athena that run queries.
    """

    def __init__(self, region='ap-southeast-2', db='default', max_queries=3, max_retries=3, timeout_minutes=10,
                 sleep_seconds=10, workgroup='primary', control_s3=None, control_key=None, parquet=None, control_data=None):
        """
        Create an AthenaClient
        :param region the AWS region to create the object
        :param max_queries the maximum number of queries to run at any one time, defaults to three
        :type max_queries int
        :param max_retries the maximum number of times execution of the query will be retried on failure
        :type max_retries int
        """
        self.athena = boto3.client(service_name='athena', region_name=region)
        self.db_name = db
        self.workgroup = workgroup
        self.control_s3 = control_s3
        self.control_key = control_key
        self.parquet = parquet
        self.control_data = control_data
        # self.interleaved_priority = False

        super(AthenaClient, self).__init__(
            max_queries, max_retries, timeout_minutes, sleep_seconds)

    def __del__(self):
        """
        when deleting the instance, ensure that all associated tasks are stopped and do not enter the queue
        """
        self.stop_and_delete_all_tasks()

    @AWSRetry.backoff(added_exceptions=["ThrottlingException"])
    def _update_task_status(self, task):
        """
        Gets the status of the query, and updates its status in the queue.
        Any queries that fail are reset to pending so they will be run a second time
        """

        logger.debug("...checking status of query {0} to {1}".format(
            task.name, task.arguments["output_location"]))
        execution_result = self.athena.get_query_execution(
            QueryExecutionId=task.id)["QueryExecution"]
        status = execution_result["Status"]
        statistics = execution_result["Statistics"]

        if task.arguments.get('hour_job'):
            logger.info(
                f"                          -> Date: {task.arguments['date_string']}, Hour: {str(task.arguments['hour_job']['hour']).zfill(2)}, Id: {task.id}, State: {task.arguments['hour_job']['state']}  -> {status['State']}, Scanned Data: {statistics.get('DataScannedInBytes' )}, Run Time: {statistics.get('EngineExecutionTimeInMillis')}, Start Time: {status['SubmissionDateTime']}  ")

            task.arguments['hour_job']['state'] = status["State"]
            task.arguments['hour_job']['startTime'] = str(
                status["SubmissionDateTime"])
            task.arguments['hour_job']['dataScannedInBytes'] = statistics.get(
                "DataScannedInBytes")
            task.arguments['hour_job']['runTimeInMillis'] = statistics.get(
                'EngineExecutionTimeInMillis')
        else:
            logger.info(
                f"""Running drop table query {task.arguments['sql']}""")

        if status["State"] == "RUNNING" or status["State"] == "QUEUED":
            task.is_complete = False
        elif status["State"] == "SUCCEEDED":
            task.is_complete = True
            # refresh and persist control dict object
            self._write_control()
        else:
            if "StateChangeReason" in status:
                task.error = status["StateChangeReason"]
            else:
                task.error = status["State"]

    def _write_control(self):
        with open("control_output.json", "w") as f:
            f.write(str(self.control_data))

        self.control_s3.put("control_output.json", self.control_key)

    def _trigger_task(self, task):
        """
        Runs a query in Athena
        """
        logger.info("Starting query {0} to {1}".format(
            task.name, task.arguments["output_location"]))

        if task.arguments.get('encryptQueryResults') and task.arguments['encryptQueryResults'].lower() != "false":
            logger.info("Running encryption..")

            task.id = self.athena.start_query_execution(
                QueryString=task.arguments["sql"],
                QueryExecutionContext={'Database': self.db_name},
                ResultConfiguration={
                    'OutputLocation': task.arguments["output_location"],
                    'EncryptionConfiguration': {
                        # 'SSE_S3'|'SSE_KMS'|'CSE_KMS'
                        'EncryptionOption': task.arguments['encryptionType'],
                        'KmsKey': task.arguments['encryptionKey']
                    }
                },
                WorkGroup=self.workgroup)["QueryExecutionId"]

            if task.arguments.get('hour_job'):
                task.arguments['hour_job']['queryid'] = task.id
        else:
            task.id = self.athena.start_query_execution(
                QueryString=task.arguments["sql"],
                QueryExecutionContext={'Database': self.db_name},
                ResultConfiguration={
                    'OutputLocation': task.arguments["output_location"]},
                WorkGroup=self.workgroup)["QueryExecutionId"]

            if task.arguments.get('hour_job'):
                task.arguments['hour_job']['queryid'] = task.id

    def add_query(self, data, sql, hour_job):
        """
        Adds a query to Athena. Respects the maximum number of queries specified when the module was created.
        Retries queries when they fail.
        Returns a query object
        :param data: the config data passed for running this query
        :param sql: the sql template read from s3
        :param hour job: a tuple with date_string and control_hour_job object
        :return:
        """

        task_name = data['controlKey'].split("/")[0]
        date_string = hour_job[0]
        control_hour_job = hour_job[1]
        hour_string = str(control_hour_job["hour"])

        control_hour_job["workgroup"] = data.get("workgroup")

        output_location = f"""{data['resultsLocation']}{date_string.split("-")[0]}/{date_string.split("-")[1]}/{date_string.split("-")[2]}/{hour_string.zfill(2)}"""

        sql = sql.replace("<date>", date_string).replace("<hour>", hour_string)

        if data.get('parquet') and data['parquet'] != "false":
            if "format='parquet'" in sql.lower():
                raise AthenaClientError(
                    "ERROR: SQL script is already creating table with parquet output. Config file cannot accept parquet again")

            if not data.get('dropTableName'):
                # raise AthenaClientError(
                #     "Cannot output in Parquet without a drop table name")
                logger.info(
                    f"Creating a temp table temp.parquet_{task_name.replace('-','_')} for parquet file output")
                self._add_drop_table_task(
                    'temp.parquet_'+task_name.replace('-', '_'), task_name, output_location)

            sql = f"""CREATE TABLE { data['dropTableName'] if data.get('dropTableName') else 'temp.parquet_'+task_name.replace('-','') }
                WITH (
                { "partitioned_by = ARRAY[" + data.get('partition_by') +  "]," if data.get('partition_by') else "" } 
                format='PARQUET',
                parquet_compression = 'SNAPPY'
                ) AS """+sql

        # for cases when the script is creating table in parquet format but parquet in config is not true
        # and when parquet in config is true, and also specify dropTableName in config
        if data.get('dropTableName'):
            self._add_drop_table_task(
                data.get('dropTableName'), task_name, output_location)

        args = {"sql": sql,
                "output_location": output_location,
                "hour_job": control_hour_job,
                "date_string": date_string,
                "parquet": data.get('parquet'),
                "dropTableName": data.get("dropTableName"),
                "encryptQueryResults": data.get("encryptQueryResults"),
                "encryptionType": data.get("encryptionType"),
                "encryptionKey": data.get("encryptionKey")
                }

        query = self.add_task(name=task_name,
                              priority=1,
                              args=args)

        return query

    def _add_drop_table_task(self, table_name, task_name, output_location):
        self.interleaved_priority = True
        self.add_task(name=task_name,
                      priority=0,
                      args={"sql": f"DROP TABLE IF EXISTS {table_name}",
                            "output_location": "s3://aws-athena-query-results-462463595486-ap-southeast-2"})

        # need clean up the files in the destination if outputing in the same path
        print(f"Output location is : {output_location}")
        prefix = "/".join(output_location.replace("s3://",
                                                  "").split("/")[1:-1])

        print(f"prefix is : {prefix}")
        files = list(self.control_s3.list_objects(prefix))
        for f in files:
            print("Deleting file : " + str(f))
            self.control_s3.delete(f)

    def wait_for_completion(self):
        """
        Check if jobs have failed, if so trigger deletion event for AthenaClient,
        else wait for completion of any queries and also any pending parquet conversions.
        Will automatically remove all pending and stop all active queries upon completion.
        """
        try:
            super(AthenaClient, self).wait_for_completion()

        except Exception as e:
            raise e
        finally:
            self.stop_and_delete_all_tasks()

    @staticmethod
    def _get_table_name(s3_target):
        path = s3_target.path.split("/")
        if path[-1] == "":
            path = path[-2]
        else:
            path = path[-1]

        return re.sub("[^A-Za-z\d]", "_", path.lower())

    def _stop_all_active_tasks(self):
        """
        iterates through active queue and stops all queries from executing
        :return: None
        """
        while self.active_queue:
            task = self.active_queue.pop()
            logger.info("Response while stop_query_execution with following QueryExecutionId {}; {}"
                        .format(task.id, self.athena.stop_query_execution(QueryExecutionId=task.id)))

    def stop_and_delete_all_tasks(self):
        """
        stops active tasks and removes pending tasks for a given client
        :return: None
        """
        self._empty_pending_queue()
        self._stop_all_active_tasks()
