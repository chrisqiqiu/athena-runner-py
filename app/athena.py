import boto3
import logging
import time
import os
import re
from .task_queue import TaskQueue, RetryException
from awsretry import AWSRetry

from .lib.log import setup_logger
from .lib.notification import SlackNotification

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
    A client for AWS Athena that will create tables from S3 buckets (using AWS Glue)
    and run queries against these tables.
    """

    def __init__(self, region, db, max_queries=3, max_retries=3, s3_parquet=None):
        """
        Create an AthenaClient
        :param region the AWS region to create the object, e.g. us-east-2
        :param db the Glue database to use
        :param max_queries the maximum number of queries to run at any one time, defaults to three
        :type max_queries int
        :param max_retries the maximum number of times execution of the query will be retried on failure
        :type max_retries int
        """
        self.athena = boto3.client(service_name='athena', region_name=region)
        self.glue = boto3.client(service_name='glue', region_name=region)
        self.db_name = db
        self.aws_region = region
        self.scp = s3_parquet

        super(AthenaClient, self).__init__(max_queries, max_retries)

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
        status = self.athena.get_query_execution(QueryExecutionId=task.id)[
            "QueryExecution"]["Status"]

        if status["State"] == "RUNNING":
            task.is_complete = False
        elif status["State"] == "SUCCEEDED":
            task.is_complete = True
            if task.arguments["parquet"]:
                logger.info("starting conversion to")
                # self.scp.convert("{0}{1}.csv".format(task.arguments["output_location"],
                #                                      task.id),
                #                  delete_csv=True,
                #                  name="convert {0}".format(task.name))
        else:
            if "StateChangeReason" in status:
                task.error = status["StateChangeReason"]
            else:
                task.error = status["State"]

    def _trigger_task(self, task):
        """
        Runs a query in Athena
        """

        logger.info("Starting query {0} to {1}".format(
            task.name, task.arguments["output_location"]))

        task.id = self.athena.start_query_execution(
            QueryString=task.arguments["sql"],
            QueryExecutionContext={'Database': self.db_name},
            ResultConfiguration={
                'OutputLocation': task.arguments["output_location"]}
        )["QueryExecutionId"]

    def add_query(self, sql, name, output_location, parquet=False):
        """
        Adds a query to Athena. Respects the maximum number of queries specified when the module was created.
        Retries queries when they fail so only use when you are sure your syntax is correct!
        Returns a query object
        :param sql: the SQL query to run
        :param name: the name which will be logged when running this query
        :param output_location: the S3 prefix where you want the results stored
        :param parquet: whether to compress to parquet when finished
        :return:
        """

        # if parquet is True and self.scp is None:
        #     raise AthenaClientError(
        #         "Cannot output in Parquet without a S3Csv2Parquet object")

        query = self.add_task(name=name,
                              args={"sql": sql,
                                    "output_location": output_location,
                                    "parquet": parquet})

        return query

    def wait_for_completion(self):
        """
        Check if jobs have failed, if so trigger deletion event for AthenaClient,
        else wait for completion of any queries and also any pending parquet conversions.
        Will automatically remove all pending and stop all active queries upon completion.
        """
        try:
            super(AthenaClient, self).wait_for_completion()
            # if self.scp is not None:
            #     self.scp.wait_for_completion()
        except Exception as e:
            raise e
        finally:
            self.stop_and_delete_all_tasks()

    def _db_exists(self):
        for database in self.glue.get_databases(MaxResults=1000)["DatabaseList"]:
            if database["Name"] == self.db_name:
                return True
        return False

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
