from datetime import date, timedelta
import numpy as np
import pandas as pd
import re
import sqlalchemy
import s3fs



class Utils():
    
    """
    Class for exporting utility methods.
    """

    @staticmethod
    def remove_leading_char(data_entry:str) -> str:
        """"Clean the naming convention for data entries of all columns"""
        clean_aff = re.sub(r'^[^A-Z]*', '', data_entry)
        if clean_aff:
            return clean_aff
    @staticmethod        
    def add_empty_cell_string(df:pd.DataFrame()) -> pd.DataFrame():
        """Replacing all empty cells with default string 'Missing data'"""
        df.fillna('Missing data', inplace=True)
        return df 
    
    @staticmethod
    def has_numbers(inputString:str) -> str:
        return bool(re.search(r'\d', inputString))


    @staticmethod
    def query_db(query:str, connection) -> pd.DataFrame:
        """
        Query a database via a given engine connection.
        """
        return pd.read_sql_query(query,connection)


    # @staticmethod
    # def retrieve_bucket(bucket_name:str) -> list:
    #     """
    #     Retrieve a list of files within a given s3 bucket
    #     """
    #     return s3fs.S3FileSystem().find(bucket_name)
    
    # @staticmethod
    # def retrieve_data(bucket:list) -> pd.DataFrame:
    #     """
    #     Search through all files in s3 bucket to find latest csv
    #     """
    #     yesterday = date.today() - timedelta(days = 1)
    #     csv_filepath = yesterday.strftime("%y/%m/%d")
    #     daily_ingestion = [file for file in bucket if csv_filepath in file][0]

    #     return  pd.read_csv(f's3://{daily_ingestion}') 

    # @staticmethod
    # def connect_to_db(username,password,host,db_name) -> sqlalchemy.engine:
    #     """
    #     Create engine to connect to aurora database for given credentials.
    #     """

    #     engine = sqlalchemy.create_engine(
    #     f"""postgresql+psycopg2://{username}:{password}@{host}/{db_name}""")
    #     return engine