import logging

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from scripts.utils.date_utils import DateUtils


class FunctionNameFilter(logging.Filter):
    def filter(self, record):
        # Get the caller's function name from the stack
        if record.levelname == "ERROR":
            frame = logging.currentframe().f_back.f_back
        else:
            frame = logging.currentframe()

        while str(record.funcName) != "log" or str(record.funcName) != "detailed":
            frame = frame.f_back
            record.funcName = frame.f_code.co_name

        frame = frame.f_back
        record.funcName = frame.f_code.co_name
        return True


class Logger:
    """
    A class to manage logging functionality.

    Attributes:
        logger (logging.Logger): The logger object.
        detailed_logging (bool): Whether to enable detailed logging.
    """

    # list to store all log statements which need to be written to S3 at the end of the process
    log_list = []
    _instance = None  # Class variable to hold the singleton instance
    partition = DateUtils.get_s3_formatted_current_date()[:14]

    def __new__(cls, *args, **kwargs):
        # Ensure that only one instance of the logger is created
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            # Initialize the logger once the instance is created
            cls._instance.__init__(*args, **kwargs)
        return cls._instance

    def __init__(self, name: str = "glue", detailed_logging: bool = False) -> None:
        """
        Initializes the logger object.

        Args:
            name (str, optional): The name of the logger. Defaults to "glue".
            detailed_logging (bool, optional): Whether to enable detailed logging. Defaults to False.
        """
        self.logger = logging.getLogger(name)

        # Set the logging level
        self.logger.setLevel(logging.INFO)

        # Configure the logging format
        formatter = logging.Formatter("%(asctime)s.%(msecs)03d|%(name)s|%(levelname)s|%(funcName)s|%(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # Create a console handler and set the formatter
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Add the filter to dynamically inject the function name into the log record
        self.logger.addFilter(FunctionNameFilter())

        # Add the console handler to the logger
        if not self.logger.hasHandlers():
            self.logger.addHandler(console_handler)

        # Set the detailed logging flag
        self.detailed_logging = detailed_logging

    def log(self, level: str, message: str) -> None:
        """
        Logs a message with the specified level.

        Args:
            level (str): The logging level. Can be one of "INFO", "DEBUG", "WARNING", or "ERROR".
            message (str): The message to log.
        """
        # Append all logs statements to a list
        log_entry = f"""{logging.Formatter('%(asctime)s.%(msecs)03d|%(name)s|%(levelname)s|%(message)s', datefmt='%Y-%m-%d %H:%M:%S').format(
            logging.LogRecord(name='glue', level=level, pathname='', lineno=0, msg=message, args=None, exc_info=None))}"""
        self.log_list.append(log_entry)
        if level.upper() == "INFO":
            self.logger.info(message)
        elif level.upper() == "DEBUG":
            self.logger.debug(message)
        elif level.upper() == "WARNING":
            self.logger.warning(message)
        elif level.upper() == "ERROR":
            self.logger.error(message)
        elif level.upper() == "EXCEPTION":
            self.logger.exception(message)
        else:
            raise ValueError("Invalid logging level.")

    def detailed(self, message: str) -> None:
        """
        Logs a message with the "DETAILED" level.

        Args:
            message (str): The message to log.
        """

        if self.detailed_logging:
            self.logger.log(logging.INFO, message)

    def upload_to_s3(self, s3_log_config):
        """Write all logs into a file in the logs bucket

        Args:
            s3_log_config (dict): contains s3 details for logging
        """
        try:
            bucket_name = s3_log_config["bucket_name"]
            system_name = s3_log_config["system_name"]
            integration_name = s3_log_config["integration_name"]
            sub_process = s3_log_config["sub_process"]
            cor_id = s3_log_config["cor_id"]

            prefix = f"{system_name}/{integration_name}/{sub_process}/{self.partition}"
            log_key = f"{prefix}/{cor_id}.log"
            log_content = "\n".join(self.log_list)

            s3_client = boto3.client("s3", region_name="ca-central-1")
            s3_client.put_object(Bucket=bucket_name, Key=log_key, Body=log_content)
            self.log(
                "info",
                f"Logs have been successfully written to s3://{bucket_name}/{log_key}",
            )
        except (NoCredentialsError, PartialCredentialsError):
            self.log("error", "Error: AWS credentials not found")
        except Exception as exc:
            self.log("error", f"An error occurred: {exc}")
