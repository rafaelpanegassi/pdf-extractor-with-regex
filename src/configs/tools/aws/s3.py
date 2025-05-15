import os

import boto3
from loguru import logger


class AWSS3:
    """
    Manages interactions with AWS S3 buckets, including uploading, downloading,
    and deleting files. Implements a Singleton pattern and uses loguru for logging.
    """
    _instance = None

    def __new__(cls, access_key=None, secret_key=None, region_name=None):
        """
        Implements the Singleton pattern for the AWSS3 class.
        Initializes the Boto3 S3 client upon the first instance creation.

        Args:
            access_key: AWS access key ID. Defaults to None, checks environment variable.
            secret_key: AWS secret access key. Defaults to None, checks environment variable.
            region_name: AWS region name. Defaults to None, checks environment variable.

        Returns:
            The single instance of the AWSS3 class.

        Raises:
            ValueError: If AWS credentials are not provided via arguments or environment variables
                        during the first instance creation.
            Exception: If Boto3 S3 client initialization fails during the first instance creation.
        """
        if cls._instance is None:
            logger.info("Creating the first instance of AWSS3.")
            cls._instance = super().__new__(cls)

            if (
                not cls._instance.check_environment_variables()
                and access_key is None
                and secret_key is None
                and region_name is None
            ):
                logger.error("AWS credentials were not provided via arguments or environment variables.")
                raise ValueError("AWS credentials were not provided.")

            cls._instance.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
            cls._instance.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
            cls._instance.region_name = region_name or os.getenv("AWS_REGION")

            if not cls._instance.access_key or not cls._instance.secret_key:
                logger.error("AWS access key or secret key is missing after checking all sources.")
                raise ValueError("AWS credentials were not provided.")

            logger.info("AWS credentials loaded successfully.")
            logger.info(f"Using region: {cls._instance.region_name}")

            try:
                cls._instance.s3 = boto3.client(
                    "s3",
                    aws_access_key_id=cls._instance.access_key,
                    aws_secret_access_key=cls._instance.secret_key,
                    region_name=cls._instance.region_name,
                )
                logger.info("Boto3 S3 client initialized successfully.")
            except Exception as e:
                logger.error(f"Error initializing Boto3 S3 client: {e}")
                cls._instance = None
                raise
        else:
            logger.info("Returning existing instance of AWSS3.")

        return cls._instance

# Dentro do arquivo configs/tools/aws/s3.py, na classe AWSS3

    def download_file_from_s3(self, bucket_name: str, key: str, local_file_path: str):
        """
        Downloads a file from an S3 bucket to a local path.
    
        Args:
            bucket_name: The name of the S3 bucket.
            key: The key (path) of the file in the S3 bucket.
            local_file_path: The local path to save the downloaded file.
        """
        logger.info(f"Attempting to download file from s3://{bucket_name}/{key} to {local_file_path}")
        try:
            with open(local_file_path, "wb") as f:
                self.s3.download_fileobj(bucket_name, key, f)
            logger.info(f"Successfully downloaded file to {local_file_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file from s3://{bucket_name}/{key} to {local_file_path}: {e}")
            return False


    def upload_file_to_s3(self, bucket_name: str, key: str, local_file_path: str) -> bool:
        """
        Uploads a local file to an S3 bucket.

        Args:
            bucket_name: The name of the S3 bucket.
            key: The key (path) to save the file in the S3 bucket.
            local_file_path: The local path of the file to upload.

        Returns:
            True if the upload is successful, False otherwise.
        """
        logger.info(f"Attempting to upload file from {local_file_path} to s3://{bucket_name}/{key}")
        try:
            self.s3.upload_file(local_file_path, bucket_name, key)
            logger.info(f"Successfully uploaded file to s3://{bucket_name}/{key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading file from {local_file_path} to s3://{bucket_name}/{key}: {e}")
            return False

    def delete_file_from_s3(self, bucket_name: str, key: str):
        """
        Deletes a file from an S3 bucket.

        Args:
            bucket_name: The name of the S3 bucket.
            key: The key (path) of the file to delete in the S3 bucket.
        """
        logger.info(f"Attempting to delete file s3://{bucket_name}/{key}")
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Successfully deleted file s3://{bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Error deleting file s3://{bucket_name}/{key}: {e}")

    @staticmethod
    def check_environment_variables() -> bool:
        """
        Checks if required AWS environment variables are set.

        Returns:
            True if AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION are set, False otherwise.
        """
        logger.info("Checking for AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION).")
        access_key_set = os.getenv("AWS_ACCESS_KEY_ID") is not None
        secret_key_set = os.getenv("AWS_SECRET_ACCESS_KEY") is not None
        region_set = os.getenv("AWS_REGION") is not None

        if not access_key_set or not secret_key_set or not region_set:
            missing_vars = [var for var, is_set in [("AWS_ACCESS_KEY_ID", access_key_set), ("AWS_SECRET_ACCESS_KEY", secret_key_set), ("AWS_REGION", region_set)] if not is_set]
            logger.warning(
                f"Missing required AWS environment variables: {', '.join(missing_vars)}"
            )
            return False
        else:
            logger.info("Required AWS environment variables configured correctly.")
            return True
