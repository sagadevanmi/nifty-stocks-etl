import datetime


class DateUtils:
    """
    Class for Date Util Functions
    """

    @staticmethod
    def get_partition_string() -> str:
        """get date partition which will be used as partition path

        Returns:
            str: datetime for partitioning
        """
        current_date = datetime.datetime.utcnow()
        formatted_date_time = current_date.strftime("%Y%m%d%H%M%S%f")
        return formatted_date_time

    @staticmethod
    def get_formatted_current_date() -> str:
        """get datetime

        Returns:
            str: datetime
        """
        current_date = datetime.datetime.utcnow()
        formatted_date_time = current_date.strftime("%Y/%m/%d %H:%M:%S:%f")
        return formatted_date_time