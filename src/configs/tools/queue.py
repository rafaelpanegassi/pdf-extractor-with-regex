import json
import os
import re
import urllib.parse

from configs.rules.notas import rules_dict
from configs.tools.aws.sqs import AWSSQSManager
from table_pdf_extractor import PDFTableExtractor
from extractor_text_pdf import PDFTextExtractor
from loguru import logger


class HTMLSQSListener:
    """
    Listens to an AWS SQS queue for messages indicating new PDF files to process.

    Each message is expected to contain information about a PDF file in S3.
    The class downloads the file, extracts both text and table data using
    separate extractors, and processes the results before deleting the message
    from the queue.
    """
    def __init__(self):
        """
        Initializes the HTMLSQSListener by getting the queue name from
        environment variables and setting up the SQS manager.
        """
        self.queue = os.getenv("QUEUE_NAME")
        self.sqs = AWSSQSManager()

    def check_messages(self):
        """
        Checks the SQS queue for messages. If messages are found, it processes
        each message sequentially.

        For each message:
        1. Parses the S3 object key from the message body.
        2. Processes the object key (unquoting and cleaning).
        3. Calls PDFTextExtractor and PDFTableExtractor to process the PDF.
        4. Logs success or failure based on the results from the extractors.
        5. Deletes the message from the queue upon completion of processing
           (either success or failure caught by the inner try/except, but before re-raising).
        If extraction fails due to an exception, the message is deleted before
        re-raising the exception.
        """
        has_message = self.sqs.check_message_in_queue(self.queue)
        if has_message:
            messages = self.sqs.receive_messages_from_queue(self.queue)

            for message in messages:
                receipt_handle = message["ReceiptHandle"]
                json_body = json.loads(message["Body"])
                object_key = json_body["Records"][0]["s3"]["object"]["key"]
                object_key_unquote = urllib.parse.unquote(object_key)
                object_key_final = re.sub(r"\+(?=\()", " ", object_key_unquote)

                try:
                    logger.info(f"Processing file: {object_key_final}")
                    resultTxt = PDFTextExtractor(object_key_final).start()
                    resultImg = PDFTableExtractor(
                        object_key_final, configs=rules_dict["jornada"]
                    ).start()
                except Exception as e:
                    self.sqs.delete_message_from_queue(self.queue, receipt_handle)
                    raise (e)
                if resultTxt and resultImg:
                    logger.info("Task processed successfully")
                else:
                    logger.warning("Task processed with failure or partial success")
                self.sqs.delete_message_from_queue(self.queue, receipt_handle)