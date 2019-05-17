from lib.log import setup_logger
from lib.notification import SlackNotification


class Task:
    """
    An abstraction representing a Single task
    """

    def __init__(self, name, priority, arguments):
        self.is_complete = False
        self.error = None
        self.arguments = arguments
        self.priority = priority
        self.id = None
        self.retries = 0
        self.name = name
