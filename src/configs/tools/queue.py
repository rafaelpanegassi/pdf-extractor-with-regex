import json
import os
import re
import urllib.parse

from loguru import logger
from table_pdf_extractor import PDFTableExtractor
from text_pdf_extractor import PDFTextExtractor

from configs.rules.notas import rules_dict
from configs.tools.aws.sqs import AWSSQSManager


class AWSSQSManager:
    def __init__(self):
        logger.info("AWSSQSManager placeholder initialized.")
        pass

    def check_message_in_queue(self, queue_name: str) -> bool:
        logger.info(f"Checking for messages in queue: {queue_name} (placeholder)")
        return True

    def receive_messages_from_queue(self, queue_name: str, max_number_of_messages: int = 10, visibility_timeout: int = 30) -> list:
        logger.info(f"Receiving messages from queue: {queue_name} (placeholder)")
        dummy_message = {
            "ReceiptHandle": "dummy_receipt_handle_123",
            "Body": json.dumps({
                "Records": [
                    {
                        "s3": {
                            "object": {
                                "key": "path/to/your/file.pdf+with+spaces(and)special%20chars"
                            }
                        }
                    }
                ]
            })
        }
        return [dummy_message]

    def delete_message_from_queue(self, queue_name: str, receipt_handle: str):
        logger.info(f"Deleting message from queue: {queue_name} with handle: {receipt_handle} (placeholder)")
        pass


rules_dict = {
    "jornada": {"some_config": "value"}
}

class PDFTextExtractor:
    def __init__(self, file_path):
        self.file_path = file_path
        logger.info(f"PDFTextExtractor initialized for file: {file_path} (placeholder)")

    def start(self) -> bool:
        logger.info(f"Starting text extraction for: {self.file_path} (placeholder)")
        return True

class PDFTableExtractor:
    def __init__(self, file_path, configs):
        self.file_path = file_path
        self.configs = configs
        logger.info(f"PDFTableExtractor initialized for file: {file_path} with configs: {configs} (placeholder)")

    def start(self) -> bool:
        logger.info(f"Starting table extraction for: {self.file_path} (placeholder)")
        return True


class HTMLSQSListener:
    """
    Listens to an SQS queue for messages related to S3 object creation,
    processes PDF files using text and table extractors, and deletes messages upon successful processing.
    Uses loguru for logging.
    """
    def __init__(self):
        """
        Initializes the HTMLSQSListener, getting the queue name from environment variables
        and initializing the AWSSQSManager.
        """
        logger.info("Initializing HTMLSQSListener.")
        self.queue = os.getenv("QUEUE_NAME")
        if not self.queue:
            logger.error("QUEUE_NAME environment variable is not set.")
        else:
             logger.info(f"SQS Queue name obtained from environment variable: {self.queue}")

        try:
            self.sqs = AWSSQSManager()
            logger.info("AWSSQSManager initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize AWSSQSManager: {e}")
            raise


    def check_messages(self):
        """
        Checks the SQS queue for messages, processes them if found, and deletes them.
        """
        if not self.queue:
            logger.warning("Cannot check messages. QUEUE_NAME is not set.")
            return

        logger.info(f"Checking SQS queue '{self.queue}' for messages.")
        has_message = self.sqs.check_message_in_queue(self.queue)

        if has_message:
            logger.info(f"Messages found in queue '{self.queue}'. Receiving messages.")
            messages = self.sqs.receive_messages_from_queue(self.queue)

            if not messages:
                 logger.info(f"check_message_in_queue indicated messages, but receive_messages_from_queue returned empty list for queue '{self.queue}'.")
                 return

            logger.info(f"Received {len(messages)} messages.")

            for message in messages:
                receipt_handle = message.get("ReceiptHandle")
                if not receipt_handle:
                    logger.warning(f"Received message without ReceiptHandle. Skipping message: {message}")
                    continue

                try:
                    json_body = json.loads(message.get("Body", "{}"))
                    records = json_body.get("Records", [])
                    if not records or len(records) == 0:
                         logger.warning(f"Message body does not contain expected 'Records' structure. Skipping message with handle: {receipt_handle}")
                         self.sqs.delete_message_from_queue(self.queue, receipt_handle)
                         continue

                    s3_object = records[0].get("s3", {}).get("object", {})
                    object_key = s3_object.get("key")

                    if not object_key:
                         logger.warning(f"Message body does not contain S3 object key. Skipping message with handle: {receipt_handle}")
                         self.sqs.delete_message_from_queue(self.queue, receipt_handle)
                         continue

                    logger.info(f"Processing message for S3 object key: {object_key}")

                    object_key_unquote = urllib.parse.unquote(object_key)
                    object_key_final = re.sub(r"\+(?=\()", " ", object_key_unquote)
                    logger.info(f"Cleaned S3 object key: {object_key_final}")

                    resultTxt = False
                    resultImg = False

                    try:
                        logger.info(f"Starting PDF text extraction for {object_key_final}")
                        resultTxt = PDFTextExtractor(object_key_final).start()
                        logger.info(f"PDF text extraction result: {resultTxt}")

                        logger.info(f"Starting PDF table extraction for {object_key_final}")
                        table_configs = rules_dict.get("jornada")
                        if table_configs is None:
                             logger.error("Configuration 'rules_dict[\"jornada\"]' not found for table extraction.")
                             pass

                        resultImg = PDFTableExtractor(
                            object_key_final, configs=table_configs
                        ).start()
                        logger.info(f"PDF table extraction result: {resultImg}")

                    except Exception as e:
                        logger.error(f"Error during PDF extraction for {object_key_final}: {e}")
                        logger.info(f"Deleting message with handle {receipt_handle} due to extraction error.")
                        self.sqs.delete_message_from_queue(self.queue, receipt_handle)
                        raise

                    if resultTxt and resultImg:
                        logger.info(f"Task processed successfully for {object_key_final}.")
                    else:
                        logger.warning(f"Task processed with partial or no success for {object_key_final}. Text success: {resultTxt}, Table success: {resultImg}")

                    logger.info(f"Deleting message with handle {receipt_handle} after processing.")
                    self.sqs.delete_message_from_queue(self.queue, receipt_handle)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON message body for message with handle {receipt_handle}: {e}")
                    self.sqs.delete_message_from_queue(self.queue, receipt_handle)
                except Exception as e:
                    logger.error(f"An unexpected error occurred while processing message with handle {receipt_handle}: {e}")
                    pass

        else:
            logger.info(f"No messages found in queue '{self.queue}'.")

