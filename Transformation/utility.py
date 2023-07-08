import sqlalchemy 
import pandas as pd



class Utils():
        
    """
    Class for exporting utility methods.
    """

    @staticmethod
    def create_db_connection(username:str,password:str,host:str,db_name:str) -> sqlalchemy.engine:
        """
        Create a connection to postgres database used throughout project
        """
        conn_string = f"""postgresql://{username}:{password}@{host}/{db_name}"""
        return sqlalchemy.create_engine(conn_string).connect()

    @staticmethod
    def fetch_db_table(table:str, connection, environment:str) -> pd.DataFrame:
        """
        Return a table from a postgres database via a given engine connection.
        """
        return pd.read_sql_table(table_name=table ,con=connection,schema=environment) 

    @staticmethod
    def query_db(query:str, connection) -> pd.DataFrame:
        """
        Query a database via a given engine connection.
        """
        return pd.read_sql_query(sql=query, conn=connection)  