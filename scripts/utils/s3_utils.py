import concurrent.futures
import json
from datetime import datetime
from typing import Any, Dict

import awswrangler as wr
import boto3
import pandas as pd
from scripts.utils.date_utils import DateUtils
from scripts.utils.log_utils import Logger

logger = Logger()


class S3Utils:
    @staticmethod
    def write_txt_to_s3(
        table_name: str,
        data: str,
        s3_config: dict,
        file_extension: str = "csv",
    ):
        """Write str to S3

        Args:
            table_name (str): Table which is being processed
            data (str): Text to be written
            s3_config (dict): S3 details

        Returns:
            Any: Exception if any
        """
        if data is None:
            return None
        bucket_name = s3_config.get("bucket_name")
        prefix = s3_config.get("prefix")
        partition = s3_config["s3_partition"].lower()
        uid = DateUtils.get_s3_formatted_current_date()

        logger.log("info", f"Write to s3 for {table_name} started")
        s3_client = boto3.client("s3", region_name="ca-central-1")
        key = f"{prefix}/{table_name}/{partition}/{table_name}_{uid}.{file_extension}"

        try:
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
            return None
        except Exception as exc:
            logger.log("exception", f"Exc: {exc} occured while writing json to S3")
            return str(exc) + " in write_txt_to_s3"

    @staticmethod
    def write_json_to_s3(
        table_name: str,
        data: json,
        s3_config: dict,
    ) -> Any:
        """Write json to S3

        Args:
            table_name (str): Table which is being processed
            data (json): json to be written
            s3_config (dict): S3 details

        Returns:
            Any: Exception if any
        """
        bucket_name = s3_config.get("bucket_name")
        prefix = s3_config.get("prefix")
        partition = s3_config["s3_partition"].lower()
        uid = DateUtils.get_s3_formatted_current_date()

        logger.log("info", f"Write to s3 for {table_name} started")
        s3_client = boto3.client("s3", region_name="ca-central-1")
        key = f"{prefix}/{table_name}/{partition}/{table_name}_{uid[:14]}.json"

        try:
            json_data = json.dumps(data)
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
            return None
        except Exception as exc:
            logger.log("exception", f"Exc: {exc} occured while writing json to S3")
            return str(exc) + " in write_json_to_s3"

    @staticmethod
    def write_df_to_s3(table_name: str, dataframe: pd.DataFrame, s3_config) -> str:
        """
        Write DataFrame to S3 bucket.

        Args:
            table_name (str): Name of the table.
            dataframe (pd.DataFrame): DataFrame to write.
            s3_config (dict): S3 details

        Returns:
            write_rv, write_exc:
        """
        if dataframe is None:
            return None
        bucket_name = s3_config.get("bucket_name")
        prefix = s3_config.get("prefix")

        partition = s3_config["s3_partition"].lower()

        logger.log("info", f"Write df to s3 for {table_name} started")
        boto3_session = boto3.session.Session(region_name="ca-central-1")

        try:
            bronze_rv = wr.s3.to_json(
                df=dataframe,
                path=f"s3://{bucket_name}/{prefix}/{table_name}/{partition}/",
                mode="append",
                boto3_session=boto3_session,
                dataset=True,
                filename_prefix=f"{table_name}_",
                index=False,
                orient="records",
                lines=True,
            )
            logger.log("info", f"S3 write for {table_name} was successful")
            return None
        except Exception as exc:
            logger.log("exception", f"Exc: {exc} while writing to S3 for {table_name}")
            return str(exc) + " in write_df_to_s3"

    @staticmethod
    def write_jsonl_to_s3(table_name: str, data: Dict, s3_config: Dict[str, str]) -> Any:
        """
        Write a JSON dictionary to an S3 bucket in JSON Lines (JSONL) format.

        Args:
            table_name (str): The name of the table being processed.
            data (Dict): The JSON dictionary to be written to S3.
            s3_config (Dict[str, str]): Configuration for S3 including the bucket name and prefix.

        Returns:
            Any: Exception if any
        """
        if data is None or data == "":
            return None
        bucket_name = s3_config.get("bucket_name")
        prefix = s3_config.get("prefix")
        partition = s3_config["s3_partition"].lower()
        uid = DateUtils.get_s3_formatted_current_date()

        logger.log("info", f"Write to s3 for {table_name} started")
        s3_client = boto3.client("s3", region_name="ca-central-1")
        key = f"{prefix}/{table_name}/{partition}/{table_name}_{uid}.json"

        try:
            # Create the JSONL content
            jsonl_content = "\n".join(json.dumps(record) for record in data)

            # Upload the JSONL content to S3
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=jsonl_content)
            return None
        except Exception as exc:
            logger.log("exception", f"Exc: {exc} occured while writing json to S3")
            return str(exc) + " in write_jsonl_to_s3"
