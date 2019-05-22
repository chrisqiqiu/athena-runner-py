
from lib.log import setup_logger
from lib.notification import SlackNotification
import datetime

import json
logger = setup_logger(__name__)


class ControlData:

    def __init__(self, control_dict, config_data):

        self.control_dict = control_dict
        self.config_data = config_data
        self.now = datetime.datetime.now()
        self.today = datetime.date.today()

        self.slackBot = SlackNotification(__name__)

        if config_data.get("controlDisabled"):
            self._append_control_date_with_control_disable_config()
        else:
            self._append_control_date_with_control_enable_config()

    def _append_control_date_with_control_disable_config(self):
        # check if the control_dict is None (e.g. no control.json file exist in s3 bucket)
        # if no control.json file , return a new controlData object with a new date list and then append today's date to it

        new_hour_job = self._hour_job_dict(self.now.hour)
        new_day_job = self._day_job_dict(str(self.now.year), str(
            self.now.month).zfill(2), str(self.now.day), [new_hour_job])

        if self.control_dict is None or self.control_dict.get("datelist") is None or len(self.control_dict["datelist"]) == 0:
            logger.info(
                "control dict does not exist or date list in control dict does not exist or empty")

            self.date_list = [new_day_job]

        # if control_dict exists, use the date_list in the dict and get the last date of the list
        # if the last date of the list is today, then add current hour to this last date's hourlist
        # if the last date of the list is prior to today, we need to add today's date to datelist and add current hour to the newly added date
        else:
            # if self.control_dict.get("datelist"):
            self.date_list = self.control_dict["datelist"]
            last_control_date = self.date_list[-1]
            last_control_date = datetime.date(
                int(last_control_date["year"]), int(last_control_date["month"]), int(last_control_date["day"]))
            today = datetime.date.today()

            if last_control_date < today:
                self.date_list.append(new_day_job)
            else:
                # last_control_date == today
                self.date_list[-1]["hourlist"].append(new_hour_job)

    def _append_control_date_with_control_enable_config(self):
        if not self.config_data.get("controlDays"):
            logger.info("Control Days not configured.")
            # self.slackBot.warn("Control Days not configured."  )
            return

        new_hour_jobs = [self._hour_job_dict(i) for i in range(24)]
        new_hour_job = self._hour_job_dict(self.now.hour)

        # if the date list is empty , construct one automatically based on appendHours config
        if self.control_dict is None or self.control_dict.get("datelist") is None or len(self.control_dict["datelist"]) == 0:
            logger.info(
                "Control enable branch: control dict does not exist or date list in control dict does not exist or empty ")
            date_to_add = self.today + \
                datetime.timedelta(days=int(self.config_data["controlDays"]))

            self.date_list = [self._day_job_dict(str(date_to_add.year), str(date_to_add.month).zfill(2), str(date_to_add.day), new_hour_jobs if self.config_data['appendHours'] and self.config_data['appendHours'] != "false" else [new_hour_job])
                              ]
        # else read in the existing date list, grab the last one and grow the list from it until it hits (today - controlDays) in config
        else:

            self.date_list = self.control_dict["datelist"]

            last_control_date = self.date_list[-1]
            last_control_date = datetime.date(int(last_control_date["year"]), int(
                last_control_date["month"]), int(last_control_date["day"]))

            while last_control_date <= (self.today + datetime.timedelta(days=int(self.config_data["controlDays"]))):
                last_control_date = last_control_date + \
                    datetime.timedelta(days=1)
                self.date_list.append(self._day_job_dict(str(last_control_date.year), str(last_control_date.month).zfill(
                    2), str(last_control_date.day), new_hour_jobs if self.config_data['appendHours'] and self.config_data['appendHours'] != "false" else [new_hour_job]))

    def _hour_job_dict(self, hour):
        return {"hour": hour,
                "queryid": "",
                "state": "",
                "dataScannedInBytes": None,
                "runTimeInMillis": None,
                "startTime": None,
                "workgroup": None
                }

    def _day_job_dict(self, year, month, day, hourlist):
        return {
            "year": year,
            "month": month,
            "day": day,
            "hourlist": hourlist
        }

    def __str__(self):
        return json.dumps({"datelist": self.date_list}, indent=4)
