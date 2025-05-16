import os

import psycopg2
from loguru import logger
from sqlalchemy import create_engine


class RDSPostgreSQLManager:
    """
    Manages connections and operations for an RDS PostgreSQL database.

    This class provides methods to connect to a PostgreSQL database,
    execute queries, and perform insertions. It can use credentials
    passed during initialization or environment variables.
    """

    def __init__(
        self, db_name=None, db_user=None, db_password=None, db_host=None, db_port="5432"
    ):
        """
        Initializes the RDSPostgreSQLManager with database credentials.

        Credentials can be provided directly or read from environment variables.
        If no credentials are provided directly and environment variables
        are not set, a ValueError is raised.

        Args:
            db_name (str, optional): The database name. Defaults to None.
            db_user (str, optional): The database user. Defaults to None.
            db_password (str, optional): The database password. Defaults to None.
            db_host (str, optional): The database host address. Defaults to None.
            db_port (str, optional): The database port. Defaults to "5432".

        Raises:
            ValueError: If no database credentials are provided either directly
                        or via environment variables.
        """
        env_vars_set = self.check_environment_variables()

        if (
            db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and not env_vars_set
        ):
            raise ValueError("Database credentials were not provided.")

        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database.

        Returns:
            psycopg2.extensions.connection or None: The database connection object
                                                     if successful, None otherwise.
        """
        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            logger.info("Successfully connected to the PostgreSQL database.")
            return connection
        except psycopg2.Error as e:
            logger.error(f"Error connecting to the PostgreSQL database: {e}")
            return None

    def execute_query(self, query):
        """
        Executes a SQL query on the database and returns the results.

        Args:
            query (str): The SQL query string to execute.

        Returns:
            list or None: A list of tuples containing the query results if successful,
                          None otherwise.
        """
        connection = self.connect()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                connection.commit()
                connection.close()
                logger.info("SQL query executed successfully.")
                return result
            except psycopg2.Error as e:
                logger.error(f"Error executing SQL query: {e}")
                if connection:
                    connection.close()
                return None
        else:
            logger.warning("Could not establish database connection to execute query.")
            return None

    def execute_insert(self, query, values):
        """
        Executes a SQL insert statement on the database.

        Args:
            query (str): The SQL insert statement string (with placeholders).
            values (tuple or list): The values to be inserted, matching the placeholders
                                    in the query.
        """
        connection = self.connect()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query, values)
                connection.commit()  # Commit the transaction
                cursor.close()
                connection.close()
                logger.info("Insertion successful.")
            except psycopg2.Error as e:
                logger.error(f"Error executing SQL insert: {e}")
                if connection:
                    connection.close()
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred during insert execution: {e}"
                )
                if connection:
                    connection.close()
        else:
            logger.warning("Could not establish database connection to execute insert.")

    @staticmethod
    def check_environment_variables():
        """
        Checks if the required database environment variables are set.

        Returns:
            bool: True if all required environment variables are set, False otherwise.
        """
        required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            logger.warning(
                f"Missing database environment variables: {', '.join(missing_vars)}"
            )
            return False
        else:
            logger.info("Database environment variables are configured correctly.")
            return True

    def alchemy(self):
        """
        Creates and returns a SQLAlchemy engine for the database.

        Returns:
            sqlalchemy.engine.base.Engine: The SQLAlchemy engine object.
        """
        db_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        logger.info(
            f"Creating SQLAlchemy engine for database: {self.db_name} on host: {self.db_host}"
        )
        self.engine = create_engine(db_url)
        return self.engine
