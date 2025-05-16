import os
import camelot
import pandas as pd
from unidecode import unidecode
from loguru import logger

from configs.rules.notas import rules_dict 
from configs.tools.aws.s3 import AWSS3
from configs.tools.postgre import RDSPostgreSQLManager


DOWNLOAD_DIR = "download"

class PDFTableExtractor:
    """
    A class to extract table data from a PDF file, process it, and load it
    into a PostgreSQL database.

    It handles downloading the PDF from S3, extracting specific tables
    using Camelot, sanitizing data, and inserting into database tables.
    """

    def __init__(self, file_name: str, configs: dict):
        """
        Initializes the PDFTableExtractor with file details and configuration.

        Args:
            file_name: The name of the PDF file (used for download and path).
            configs: A dictionary containing configurations for extraction
                     (table areas, columns, flavor, pages, passwords, etc.).
        """
        self.file_name = file_name
        self.configs = configs
        self.aws = AWSS3()
        self.download_path = os.path.join(DOWNLOAD_DIR, self.file_name)

    def start(self) -> bool:
        """
        Starts the extraction, processing, and loading process for the PDF.

        Manages the workflow including downloading, table extraction, data
        transformation, and database insertion. Includes error handling
        and file cleanup.

        Returns:
            True if the process completes successfully, False otherwise.
        """
        logger.info(f"Starting process for file: {self.file_name}")
        file_downloaded = False

        try:
            self.download_file()
            file_downloaded = True
            logger.info(f"File downloaded successfully: {self.download_path}")

            logger.info("Extracting tables from PDF...")
            header_df = self.get_table_data(
                self.configs.get("header_table_areas"),
                self.configs.get("header_columns"),
                self.configs.get("header_fix", True),
                flavor=self.configs.get("header_flavor", self.configs["flavor"]),
                pages=self.configs.get("header_pages", self.configs["pages"]),
            )

            main_df = self.get_table_data(
                self.configs.get("table_areas"),
                self.configs.get("columns"),
                self.configs.get("fix", True),
                 flavor=self.configs.get("main_flavor", self.configs["flavor"]),
                 pages=self.configs.get("main_pages", self.configs["pages"]),
            )

            small_df = self.get_table_data(
                self.configs.get("small_table_areas"),
                self.configs.get("small_columns"),
                self.configs.get("small_fix", True),
                flavor=self.configs.get("small_flavor", self.configs["flavor"]),
                pages=self.configs.get("small_pages", self.configs["pages"]),
            )
            logger.info("Table extraction complete.")

            if main_df is None or main_df.empty:
                 logger.warning("No main table data extracted. Skipping further processing for main.")
            else:
                logger.info("Adding header info and insertion date to main data...")
                main_df = self.add_header_info(header_df, main_df)

                logger.info("Sanitizing main table column names...")
                main_df = self.sanitize_column_names(main_df)

                main_table_name = f"fatura_{self.configs.get('name', 'default_main')}".lower()
                logger.info(f"Sending main data to DB table: {main_table_name}")
                self.send_to_db(main_df, main_table_name)
                logger.info(f"Successfully sent main data to {main_table_name}")


            if small_df is None or small_df.empty:
                 logger.warning("No small table data extracted. Skipping further processing for small.")
            else:
                logger.info("Adding header info and insertion date to small data...")
                small_df = self.add_header_info(header_df, small_df)

                if self.configs.get("small_sanitize", True):
                    logger.info("Sanitizing small table column names...")
                    small_df = self.sanitize_column_names(small_df)

                small_table_name = f"fatura_{self.configs.get('name', 'default_small')}_small".lower()
                logger.info(f"Sending small data to DB table: {small_table_name}")
                self.send_to_db(small_df, small_table_name)
                logger.info(f"Successfully sent small data to {small_table_name}")


            logger.info(f"Process completed successfully for file: {self.file_name}")
            return True

        except FileNotFoundError:
             logger.exception(f"Error: Downloaded file not found at {self.download_path}")
             return False
        except Exception as e:
            logger.exception(f"An error occurred during processing file: {self.file_name}")
            return False
        finally:
            if file_downloaded and os.path.exists(self.download_path):
                try:
                    os.remove(self.download_path)
                    logger.info(f"Cleaned up downloaded file: {self.download_path}")
                except OSError as e:
                    logger.error(f"Error removing downloaded file {self.download_path}: {e}")


    def download_file(self):
        """
        Downloads the PDF file from the configured AWS S3 bucket.
        Creates the local download directory if it doesn't exist.
        """
        bucket = os.getenv("AWS_BUCKET")
        if not bucket:
            raise ValueError("AWS_BUCKET environment variable not set.")

        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            logger.info(f"Created download directory: {DOWNLOAD_DIR}")

        logger.info(f"Attempting to download file '{self.file_name}' from S3 bucket '{bucket}' to '{self.download_path}'")
        self.aws.download_file_from_s3(
            bucket, self.file_name, self.download_path
        )

    def get_table_data(self, table_areas: list[str] | None, table_columns: list[str] | None, fix_header: bool = True, flavor: str = "stream", pages: str = "all", password: str | None = None) -> pd.DataFrame | None:
        """
        Extracts table data from the downloaded PDF using Camelot.

        Args:
            table_areas: A list of strings defining the areas on the page(s)
                         where tables are located. Can be None if no specific
                         area is needed (though Camelot usually needs this for stream).
            table_columns: A list of strings defining column separators for
                           stream mode. Can be None.
            fix_header: If True, uses the first row of the extracted table as
                        the header and drops the original first row and first column.
            flavor: The Camelot extraction method ('lattice' or 'stream'). Defaults to 'stream'.
            pages: The pages to extract tables from (e.g., '1', '1,3', '1-end'). Defaults to 'all'.
            password: The PDF password if the file is encrypted. Defaults to None.


        Returns:
            A pandas DataFrame containing the extracted and potentially
            header-fixed table data, or None if no tables were found.
        """
        if not os.path.exists(self.download_path):
             logger.error(f"PDF file not found for extraction: {self.download_path}")
             return None

        logger.info(f"Extracting table data with flavor='{flavor}', pages='{pages}', areas={table_areas}, columns={table_columns}")

        try:
            camelot_args = {
                "flavor": flavor,
                "strip_text": self.configs.get("strip_text"),
                "pages": pages,
                "password": password,
            }
            if flavor == "stream":
                 if table_columns:
                     camelot_args["columns"] = table_columns
                 if table_areas:
                      camelot_args["table_areas"] = table_areas

            elif flavor == "lattice":
                 if table_areas:
                      camelot_args["table_areas"] = table_areas
            camelot_args = {
                "flavor": flavor,
                "strip_text": self.configs.get("strip_text"),
                "pages": pages,
                "password": password,
            }
            if table_areas is not None:
                camelot_args["table_areas"] = table_areas
            if table_columns is not None:
                 camelot_args["columns"] = table_columns


            tables = camelot.read_pdf(
                self.download_path,
                **camelot_args
            )
            logger.info(f"Camelot found {tables.n} tables.")

            if tables.n == 0:
                logger.warning("No tables detected by Camelot for these settings.")
                return None

            table_content_list = []
            for page_index, table in enumerate(tables):
                 df = table.df
                 if fix_header:
                     logger.debug(f"Fixing header for table from page {table.page}, index {page_index}...")
                     df = self.fix_header(df)
                 table_content_list.append(df)

            if not table_content_list:
                 return None

            result_df = pd.concat(table_content_list, ignore_index=True)
            logger.info(f"Concatenated table data shape: {result_df.shape}")
            return result_df

        except ValueError as ve:
             logger.exception(f"ValueError during table extraction: {ve}")
             return None
        except Exception as e:
             logger.exception(f"An unexpected error occurred during table extraction: {e}")
             return None


    def add_header_info(self, header_df: pd.DataFrame | None, content_df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds information from the header DataFrame as new columns to the
        content DataFrame. Also adds an 'Insertion Date' column.

        Args:
            header_df: A pandas DataFrame containing the header information
                       (expected to have one relevant row after fixing). Can be None.
            content_df: The pandas DataFrame containing the main or small table data.

        Returns:
            The content DataFrame with header information columns and 'Insertion Date'.
        """
        if header_df is None or header_df.empty:
            logger.warning("Header DataFrame is empty or None. Cannot add header info to content.")
            content_df["insertion_date"] = pd.Timestamp("today").normalize()
            return content_df

        if content_df.empty:
            logger.warning("Content DataFrame is empty. Cannot add header info.")
            header_cols = header_df.columns.tolist()
            empty_df = pd.DataFrame(columns=header_cols + ["insertion_date"])
            return empty_df


        header_info_row = header_df.iloc[0]

        header_info_df = pd.DataFrame([header_info_row.values] * len(content_df), columns=header_df.columns)

        combined_df = pd.concat(
            [content_df.reset_index(drop=True), header_info_df.reset_index(drop=True)], axis=1
        )

        combined_df["insertion_date"] = pd.Timestamp("today").normalize()

        logger.info(f"Added header info and insertion date. New shape: {combined_df.shape}")
        return combined_df

    @staticmethod
    def fix_header(df: pd.DataFrame) -> pd.DataFrame:
        """
        Sets the first row of the DataFrame as the column headers and removes
        the original first row and the first column.

        Args:
            df: The input pandas DataFrame.

        Returns:
            A new DataFrame with the first row as header and the first row
            and first column dropped.
        """
        if df.empty:
             logger.warning("Attempted to fix header on an empty DataFrame.")
             return df
        if df.shape[0] < 2:
             logger.warning(f"DataFrame has less than 2 rows ({df.shape[0]}). Cannot fix header.")
             if isinstance(df.columns, pd.RangeIndex):
                 logger.warning("DataFrame has default columns and few rows. Header fixing skipped.")
                 return df
             else:
                 logger.warning("DataFrame has custom columns but few rows. Header fixing skipped.")
                 return df


        new_columns = df.iloc[0].astype(str)

        df = df[1:].copy()

        df.columns = new_columns

        if df.shape[1] > 0:
             col_to_drop = df.columns[0]
             df = df.drop(columns=[col_to_drop])
             logger.debug(f"Dropped first column '{col_to_drop}' after fixing header.")
        else:
             logger.warning("DataFrame has no columns after setting header. Cannot drop first column.")


        df = df.reset_index(drop=True)

        logger.debug("Header fixing complete.")
        return df


    def sanitize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitizes DataFrame column names by removing accents, replacing spaces
        with underscores, removing non-alphanumeric characters, and lowercasing.

        Args:
            df: The input pandas DataFrame.

        Returns:
            A new DataFrame with sanitized column names.
        """
        if df.empty:
             logger.warning("Attempted to sanitize columns on an empty DataFrame.")
             return df

        original_columns = list(df.columns)
        new_columns = []
        for col in original_columns:
            col_str = str(col)
            col_str = unidecode(col_str)
            col_str = col_str.replace(" ", "_")
            col_str = pd.Series(col_str).str.replace(r"[^\w]+", "", regex=True).iloc[0]
            col_str = col_str.lower()
            new_columns.append(col_str)

        df.columns = new_columns
        logger.debug(f"Sanitized columns: {original_columns} -> {new_columns}")
        return df

    def send_to_db(self, df: pd.DataFrame, table_name: str):
        """
        Sends the DataFrame content to a specified PostgreSQL table.

        Args:
            df: The pandas DataFrame to send.
            table_name: The name of the database table.
        """
        if df.empty:
             logger.warning(f"DataFrame is empty. Skipping send to DB table: {table_name}")
             return

        logger.info(f"Connecting to database and attempting to save data to table: {table_name}")
        try:
            connection = RDSPostgreSQLManager().alchemy()
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logger.success(f"Successfully saved {len(df)} rows to database table: {table_name}")
        except Exception as e:
            logger.exception(f"Failed to save data to database table: {table_name}")
            raise e
