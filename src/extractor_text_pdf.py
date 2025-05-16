import os
import re
import sys

import pandas as pd
import PyPDF2
from loguru import logger

from configs.tools.aws.s3 import AWSS3
from configs.tools.postgre import RDSPostgreSQLManager

logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")


class PDFTextExtractor:
    def __init__(self, pdf_file_path):
        self.pdf_file_path = pdf_file_path
        self.extracted_text = ""
        self.aws = AWSS3()
        logger.info(f"PDFTextExtractor initialized for file: {pdf_file_path}")

    def start(self):
        """
        Starts the PDF text extraction, transformation, and database loading process.
        """
        logger.info("Starting PDF extraction and loading process.")
        try:
            extracted_text_list = self.extract_text()
            dataframe = self.text_to_dataframe(extracted_text_list)
            self.send_to_db(dataframe, "pdf_text")
            logger.info("Process completed successfully.")
            return True
        except Exception as e:
            logger.exception(f"An error occurred during the process: {e}")
            return False

    def extract_text(self):
        """
        Downloads the PDF file and extracts text content page by page.
        Then extracts relevant operations text and splits it by newline.
        """
        self.download_file()
        pdf_local_path = f"download/{self.pdf_file_path}"
        logger.info(f"Opening PDF file: {pdf_local_path}")
        with open(pdf_local_path, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            logger.info(
                f"Successfully opened PDF. Number of pages: {len(pdf_reader.pages)}"
            )

            extracted_text = ""
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()
                logger.debug(f"Extracted text from page {page_num + 1}")

        extracted_operations_text = self.extract_operations(extracted_text)
        text_lines = self.split_text_by_newline(extracted_operations_text)

        logger.info("Text extraction and initial processing complete.")
        return text_lines

    def split_text_by_newline(self, text):
        """
        Splits the text by newline characters and returns as a list of strings.
        Handles empty input text.
        """
        if text:
            logger.debug("Splitting text by newline.")
            return text.split("\n")
        else:
            logger.warning("Input text is empty, returning empty list.")
            return []

    def extract_operations(self, text):
        """
        Extracts the section of text containing operations using a regex pattern.
        """
        pattern = r"(C/V.*?)(?=\nPosição Ajuste)"
        logger.debug(f"Attempting to extract operations using pattern: {pattern}")

        result = re.search(pattern, text, re.DOTALL)

        if result:
            logger.info("Operations text pattern found.")
            return result.group(1)
        else:
            logger.warning("Operations text pattern not found.")
            return "Pattern not found."

    def text_to_dataframe(self, operations_text_list):
        """
        Converts the list of operation text lines into a pandas DataFrame.
        Assumes the first line is the header.
        """
        if not operations_text_list:
            logger.warning("No operations text provided to convert to DataFrame.")
            return pd.DataFrame()

        header = operations_text_list[0].split()
        data = [line.split() for line in operations_text_list[1:] if line]

        logger.info(
            f"Creating DataFrame with header: {header} and {len(data)} data rows."
        )
        dataframe = pd.DataFrame(data, columns=header)

        logger.debug("DataFrame created successfully.")
        return dataframe

    def get_text(self):
        """
        Extracts and returns the processed text content.
        """
        logger.info("Getting extracted text.")
        extracted_text_list = self.extract_text()
        return extracted_text_list

    def get_df(self):
        """
        Extracts text and converts it into a pandas DataFrame.
        """
        logger.info("Getting extracted text and converting to DataFrame.")
        extracted_text_list = self.get_text()
        return self.text_to_dataframe(extracted_text_list)

    def download_file(self):
        """
        Downloads the PDF file from the configured AWS S3 bucket.
        Creates a local 'download' directory if it doesn't exist.
        Logs the download process steps.

        Returns:
            The result of the self.aws.download_file_from_s3 call.
            (Assumed to be boolean True/False based on previous context, but could be anything).
        """
        logger.info(f"Starting download process for file: {self.pdf_file_path}")

        bucket = os.getenv("AWS_BUCKET")
        if not bucket:
            logger.error("AWS_BUCKET environment variable not set. Download skipped.")
            return False

        local_dir = "download"
        local_file_path = os.path.join(local_dir, self.pdf_file_path)

        if not os.path.exists(local_dir):
            logger.info(f"Creating local download directory: {local_dir}")
            try:
                os.makedirs(local_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"Failed to create local directory {local_dir}: {e}")
                return False

        logger.info(
            f"Calling S3 download method for '{self.pdf_file_path}' from bucket '{bucket}'"
        )
        try:
            result = self.aws.download_file_from_s3(
                bucket, self.pdf_file_path, local_file_path
            )

            if result:
                logger.info(
                    f"S3 download call completed successfully for '{self.pdf_file_path}'."
                )
            else:
                logger.error(f"S3 download call failed for '{self.pdf_file_path}'.")

            return result

        except Exception as e:
            logger.exception(
                f"An unexpected error occurred during S3 download call for '{self.pdf_file_path}'"
            )
            return False

    def send_to_db(self, dataframe, table_name):
        """
        Saves the DataFrame to the specified PostgreSQL table and removes the local PDF file.
        """
        if dataframe.empty:
            logger.warning(
                f"DataFrame is empty. Skipping save to database table: {table_name}"
            )
            return

        logger.info(f"Attempting to save data to database table: {table_name}")
        connection = None
        try:
            db_manager = RDSPostgreSQLManager()
            connection = db_manager.alchemy()
            dataframe.to_sql(table_name, connection, if_exists="append", index=False)
            logger.success(f"Successfully saved data to database table: {table_name}")

            pdf_local_path = f"download/{self.pdf_file_path}"
            if os.path.exists(pdf_local_path):
                os.remove(pdf_local_path)
                logger.info(f"Removed local PDF file: {pdf_local_path}")
            else:
                logger.warning(f"Local PDF file not found to remove: {pdf_local_path}")

        except Exception as e:
            logger.exception(f"Error saving data to database table {table_name}: {e}")
        finally:
            if connection:
                connection.dispose()
                logger.debug("Database connection closed.")
