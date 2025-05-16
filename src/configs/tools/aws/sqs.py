import os

import boto3
from loguru import logger


class AWSSQSManager:
    """
    Manages interactions with AWS SQS queues, including getting queue URLs,
    receiving, checking, and deleting messages. Uses loguru for logging.
    """

    def __init__(
        self,
        access_key: str = None,
        secret_key: str = None,
        region_name: str = None,
    ):
        """
        Initializes the AWSSQSManager.

        Args:
            access_key: AWS access key ID. Defaults to None, checks environment variable.
            secret_key: AWS secret access key. Defaults to None, checks environment variable.
            region_name: AWS region name. Defaults to None, checks environment variable.

        Raises:
            ValueError: If AWS credentials are not provided via arguments or environment variables.
            Exception: If Boto3 SQS client initialization fails.
        """
        logger.info("Initializing AWSSQSManager.")

        if (
            not self.check_environment_variables()
            and access_key is None
            and secret_key is None
            and region_name is None
        ):
            logger.error("AWS credentials were not provided.")
            raise ValueError("AWS credentials were not provided.")

        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = region_name or os.getenv("AWS_REGION")

        if not self.access_key or not self.secret_key:
            logger.error(
                "AWS credentials were not provided after environment variable check."
            )
            raise ValueError("AWS credentials were not provided.")

        logger.info("AWS credentials loaded successfully.")

        try:
            self.sqs = boto3.client(
                "sqs",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name,
            )
            logger.info("Boto3 SQS client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Boto3 SQS client: {e}")
            raise

    def get_queue_url(self, queue_name: str) -> str | None:
        """
        Gets the URL for a given SQS queue name.

        Args:
            queue_name: The name of the SQS queue.

        Returns:
            The queue URL if successful, otherwise None.
        """
        logger.info(f"Attempting to get URL for queue: {queue_name}")
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            queue_url = response["QueueUrl"]
            logger.info(
                f"Successfully retrieved URL for queue {queue_name}: {queue_url}"
            )
            return queue_url
        except Exception as e:
            logger.error(f"Error getting queue URL for {queue_name}: {e}")
            return None

    def receive_messages_from_queue(
        self,
        queue_name: str,
        max_number_of_messages: int = 10,
        visibility_timeout: int = 30,
    ) -> list:
        """
        Receives messages from an SQS queue.

        Args:
            queue_name: The name of the SQS queue.
            max_number_of_messages: The maximum number of messages to retrieve (up to 10).
            visibility_timeout: The duration (in seconds) that the received messages
                                are hidden from subsequent retrieve requests.

        Returns:
            A list of received messages, or an empty list if an error occurs or no messages are available.
        """
        logger.info(f"Attempting to receive messages from queue: {queue_name}")
        try:
            queue_url = self.get_queue_url(queue_name)
            if not queue_url:
                logger.warning(
                    f"Could not get queue URL for {queue_name}. Cannot receive messages."
                )
                return []

            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_number_of_messages,
                VisibilityTimeout=visibility_timeout,
                WaitTimeSeconds=0,
            )
            messages = response.get("Messages", [])
            logger.info(f"Received {len(messages)} messages from queue: {queue_name}")
            return messages
        except Exception as e:
            logger.error(f"Error receiving messages from queue {queue_name}: {e}")
            return []

    def check_message_in_queue(self, queue_name: str) -> bool:
        """
        Checks if there are any messages in the SQS queue.

        Args:
            queue_name: The name of the SQS queue.

        Returns:
            True if there are approximate messages in the queue, False otherwise or on error.
        """
        logger.info(f"Attempting to check message count in queue: {queue_name}")
        try:
            queue_url = self.get_queue_url(queue_name)
            if not queue_url:
                logger.warning(
                    f"Could not get queue URL for {queue_name}. Cannot check message count."
                )
                return False

            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            approximate_number_of_messages = response.get("Attributes", {}).get(
                "ApproximateNumberOfMessages", "N/A"
            )
            logger.info(
                f"Approximate number of messages in queue {queue_name}: {approximate_number_of_messages}"
            )
            if (
                approximate_number_of_messages != "N/A"
                and int(approximate_number_of_messages) > 0
            ):
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking messages in queue {queue_name}: {e}")
            return False

    def delete_message_from_queue(self, queue_name: str, receipt_handle: str):
        """
        Deletes a message from an SQS queue using its receipt handle.

        Args:
            queue_name: The name of the SQS queue.
            receipt_handle: The receipt handle of the message to delete.
        """
        logger.info(
            f"Attempting to delete message from queue: {queue_name} with receipt handle: {receipt_handle}"
        )
        try:
            queue_url = self.get_queue_url(queue_name)
            if not queue_url:
                logger.warning(
                    f"Could not get queue URL for {queue_name}. Cannot delete message."
                )
                return

            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            logger.info(
                f"Message deleted successfully from queue {queue_name} with receipt handle: {receipt_handle}."
            )
        except Exception as e:
            logger.error(f"Error deleting message from queue {queue_name}: {e}")

    @staticmethod
    def check_environment_variables() -> bool:
        """
        Checks if required AWS environment variables are set.

        Returns:
            True if all required environment variables are set, False otherwise.
        """
        logger.info(
            "Checking for AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)."
        )
        if (
            not os.getenv("AWS_ACCESS_KEY_ID")
            or not os.getenv("AWS_SECRET_ACCESS_KEY")
            or not os.getenv("AWS_REGION")
        ):
            logger.warning(
                "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or AWS_REGION environment variables are not set."
            )
            return False
        else:
            logger.info("Environment variables configured correctly.")
            return True
