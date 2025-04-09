import json
import os
from typing import Any, Dict

import awswrangler as wr
import boto3
import pandas as pd
from constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from utils.date_utils import DateUtils
from utils.log_utils import Logger

logger = Logger().set_logger()


class S3Utils:
    @staticmethod
    def write_txt_to_s3(
        table_name: str,
        data: str,
        s3_config: dict,
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

        logger.info(f"Write to s3 for {table_name} started")
        s3_client = boto3.client("s3", region_name="ca-central-1")
        key = f"{prefix}/{table_name}/{partition}/{table_name}_{uid}.csv"

        try:
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
            return None
        except Exception as exc:
            logger.exception(f"Exc: {exc} occured while writing json to S3")
            return str(exc) + " in write_txt_to_s3"

    @staticmethod
    def write_json_to_s3(
        table_name: str,
        data: json,
        bucket_name: dict,
        source_name: str,
        prefix: str,
        s3_client,
    ) -> Any:
        """Write json to S3

        Args:
            table_name (str): Table which is being processed
            data (json): json to be written
            s3_config (dict): S3 details

        Returns:
            Any: Exception if any
        """
        date_partition = DateUtils.get_partition_string()[:12]
        uid = DateUtils.get_partition_string()

        key = f"{prefix}/{source_name}/{table_name}/{date_partition}/{table_name}_{uid[:17]}.json"

        try:
            json_data = json.dumps(data)
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
            logger.info(f"Write successful for {table_name}")
            return None
        except Exception as exc:
            logger.exception(f"{exc} occured while writing json to S3")
            return str(exc) + " in write_json_to_s3"

    @staticmethod
    def write_jsonl_to_s3(
        table_name: str, data: Dict, s3_config: Dict[str, str]
    ) -> Any:
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

        logger.info(f"Write to s3 for {table_name} started")
        s3_client = boto3.client("s3", region_name="ca-central-1")
        key = f"{prefix}/{table_name}/{partition}/{table_name}_{uid}.json"

        try:
            # Create the JSONL content
            jsonl_content = "\n".join(json.dumps(record) for record in data)

            # Upload the JSONL content to S3
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=jsonl_content)
            logger.info(f"Write successful for {table_name}")
            return None
        except Exception as exc:
            logger.exception(f"{exc} occured while writing json to S3")
            return str(exc) + " in write_jsonl_to_s3"

    @staticmethod
    def write_df_to_s3(
        table_name: str,
        dataframe: pd.DataFrame,
        bucket_name: dict,
        source_name: str,
        prefix: str,
        s3_client,
    ) -> str:
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

        date_partition = DateUtils.get_partition_string()[:12]
        boto3_session = boto3.session.Session(
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        try:
            bronze_rv = wr.s3.to_csv(
                df=dataframe,
                path=f"s3://{bucket_name}/{prefix}/{source_name}/{table_name}/{date_partition}/",
                mode="append",
                boto3_session=boto3_session,
                dataset=True,
                filename_prefix=f"{table_name}_",
                index=False,
            )
            logger.info(f"S3 write for {table_name} was successful")
            return None
        except Exception as exc:
            logger.exception(f"{exc} while writing df to S3 for {table_name}")
            return str(exc) + " in write_df_to_s3"

    @staticmethod
    def write_to_s3(
        table_name: str,
        data: Any,
        s3_config: dict,
        source_name: str,
        local_run: bool = False,
    ):
        if local_run:
            os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
            os.environ["AWS_ENDPOINT_URL_S3"] = "http://localhost:9000"

            logger.info("Minio backend")
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
            )

        else:
            s3_client = boto3.client("s3", region_name="ca-central-1")

        bucket_name = s3_config.get("bucket_name")
        prefix = s3_config.get("prefix")

        logger.info(f"Write to s3 for {table_name} started")
        if isinstance(data, list):
            error = S3Utils.write_json_to_s3(
                table_name, data, bucket_name, source_name, prefix, s3_client
            )
        elif isinstance(data, pd.DataFrame):
            error = S3Utils.write_df_to_s3(
                table_name, data, bucket_name, source_name, prefix, s3_client
            )

        return error