import sqlalchemy as db
import pandas as pd



class Utils():
        
    """
    Class for exporting utility methods.
    """

    @staticmethod
    def create_db_connection(username:str,password:str,host:str,db_name:str) -> db.engine:
        """
        Create a connection to postgres database used throughout project
        """
        conn_string = f"""postgresql://{username}:{password}@{host}/{db_name}"""
        return db.create_engine(conn_string).connect()

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
    
    @staticmethod
    def upload_data(df, connection, table_name, environment):
            """
            Upload dataframe to Postgres server using sqlalchemy engine
            """
            return df.to_sql(name=table_name, con=connection, if_exists='replace', schema=environment)