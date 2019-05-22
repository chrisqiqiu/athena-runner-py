from lib.log import setup_logger
from lib.notification import SlackNotification
import time
import queue
from task import Task
import datetime

logger = setup_logger(__name__)
slackbot = SlackNotification(__name__)


class RetryException(Exception):
    def __init__(self, reason):
        Exception.__init__(
            self, 'The task exceeded the retry limit: {0}'.format(reason))
        self.reason = reason


class TaskQueue:
    """
    This is an abstract class that contains all required common functionality to
    support implementation of queues in aws client libraries.
    Two separate queues are maintained:
    active_queue - Only tasks in this queue are allowed to run.
                   Once tasks are completed they are removed from this queue.
                   No. of tasks in active queue <= max_size.
    pending_queue - Contains tasks that are awaiting execution.
                    Tasks from pending_queue are added to active_queue in FIFO
                    fashion.
    """

    def __init__(self, max_size, retry_limit=3, timeout_minutes=10, sleep_seconds=10):
        self.pending_tasks = []
        self.active_queue = []
        self.max_size = int(max_size)
        self.retry_limit = retry_limit
        self.timeout_minutes = int(timeout_minutes)
        self.timeout_seconds = int(timeout_minutes)*60
        self.sleep_seconds = int(sleep_seconds)
        self.interleaved_priority = False

    def add_task(self, name, priority, args):
        """This method adds a tasks to the pending_tasks queue"""

        task = Task(name, priority, args)
        self.pending_tasks.append(task)

        return task

    # check status of each task in active queue and update its status based on if it has error
    def _empty_active_queue(self):
        """
        Removes completed task from active queue and populates freed spots with tasks
        in the front of pending_tasks queue
        """
        logger.info(
            "[Athena Runner Step 4.1/5] check queries status for tasks in active queue... ")

        # Remove completed tasks from active queue
        for index, task in enumerate(self.active_queue):
            self._update_task_status(task)
            if task.error:
                if task.retries < self.retry_limit:
                    logger.info("Retrying job {0}, previously raised error with error {1}".
                                format(task.name, task.error))
                    task.retries += 1
                    task.error = None
                    self._trigger_task(task)
                else:
                    task.is_complete = True
                    self.active_queue.pop(index)
                    raise RetryException("{0} [name: '{1}', id: {2}]".format(
                                         task.error, task.name, task.id))

            if task.is_complete:
                if task.error:
                    logger.error("Task failed: ID {0}, error is {1}".format(
                        task.id, task.error))
                else:
                    logger.info("Task is completed: ID {0}".format(task.id))
                self.active_queue.pop(index)

        self._log_priorities_status_in_both_queues()

    def _running_jobs(self, job_name):
        """
        Return the number of concurrent jobs
        """
        running_jobs = 0
        for each_job in self.active_queue:
            if each_job.name == job_name:
                running_jobs += 1
        return running_jobs

    @property
    def number_active(self):
        return len(self.active_queue)

    @property
    def number_pending(self):
        return len(self.pending_tasks)

    @property
    def remaining_queries(self):
        return len(self.pending_tasks)+len(self.active_queue)

    @property
    def max_priority_in_active_queue(self):
        if len(self.active_queue) == 0:
            return 999
        return max(list(map(lambda task: task.priority, self.active_queue)))

    def _fill_active_queue(self):

        logger.info("[Athena Runner Step 4.2/5] move task from pending queue to active queue if there's task in pending queue and execute queries in the tasks ... ")
        # Add add tasks to active queue if size of queue is less the max query limit
        for i in range(0, self.max_size - self.number_active):
            if len(self.pending_tasks) > 0:
                task = self.pending_tasks[0]
                logger.info("\nmax_priority_in_active_queue is " +
                            str(self.max_priority_in_active_queue))
                if task.priority <= self.max_priority_in_active_queue:
                    if self.interleaved_priority:
                        if len(self.active_queue) == 0:
                            self.active_queue.append(task)
                            self.pending_tasks.pop(0)
                            self._trigger_task(task)
                        else:
                            logger.info(
                                f"pending at {task.priority} due to interleaved priority requirement")

                            self._log_priorities_status_in_both_queues()
                    else:
                        self.active_queue.append(task)
                        self.pending_tasks.pop(0)
                        self._trigger_task(task)
                else:
                    logger.info(
                        f"pending at {task.priority} due to priority > max priority")
                    self._log_priorities_status_in_both_queues()
            else:
                break
        self._log_priorities_status_in_both_queues()

    def _log_priorities_status_in_both_queues(self):
        logger.info("Current active queue is : " +
                    str(list(map(lambda task: task.priority, self.active_queue))))
        logger.info("Current pending queue is : " +
                    str(list(map(lambda task: task.priority, self.pending_tasks))))

    def wait_for_completion(self):
        """
        This method runs until execution of all tasks is completed
        """

        start_time = datetime.datetime.now()

        while self.number_active > 0 or self.number_pending > 0:
            logger.info("{} active tasks are awaiting execution".format(
                self.number_active))
            logger.info(f" ^ queries remaining {self.remaining_queries}")

            if (datetime.datetime.now() - start_time).total_seconds() > self.timeout_seconds:
                msg = "Timeout. Execution took longer than {} minutes".format(
                    str(self.timeout_minutes))
                logger.info(msg)
                slackbot.warn(msg)
                break
            self._empty_active_queue()
            self._fill_active_queue()
            msg = f" ~ sleeping for {str(self.sleep_seconds)}"
            logger.info(msg)
            time.sleep(self.sleep_seconds)

        logger.info("Done")

    def _trigger_task(self, task):
        """
           This function should implement functionality to start aws task
           Once triggered is_active should be set to True
        """
        raise NotImplementedError("must be implemented by subclass")

    def _update_task_status(self, task):
        """Updates task status and returns updated task object
           Also handles retries
        """
        raise NotImplementedError("must be implemented by subclass")

    def _empty_pending_queue(self):
        """
        Empty pending queue of tasks - prevent them from being run
        :return: None
        """
        while len(self.pending_tasks) > 0:
            self.pending_tasks.pop()
