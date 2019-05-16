from lib.log import setup_logger
from lib.notification import SlackNotification


class Task:
    """
    An abstraction representing a Single task
    """

    def __init__(self, name, arguments):
        self.is_complete = False
        self.error = None
        self.arguments = arguments
        self.id = None
        self.retries = 0
        self.name = name
        self.control_date_job = None
        self.control_hour_job = None
