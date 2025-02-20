import mysql.connector
import pandas as pd


class MySQL:
    """
    Represents a MySQL database interaction utility.

    Methods:
        connect(): Establishes a connection to the MySQL database.
        write_list(table, columns, values): Writes a list of values into the specified table.
        get_table(table, column, sort_by=None, max_entries=None, descending=False): 
            Retrieves data from a table in the specified database with optional sorting and limiting.
        get_dataframe(table, column): Returns a DataFrame of specified columns from a table.
    """
    def __init__(self, mysql_host, mysql_user, mysql_password, mysql_database):
        self.host = mysql_host
        self.user = mysql_user
        self.password = mysql_password
        self.database = mysql_database
    
    def connect(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )

    def create_table(self, table, columns, types):
        """
        Creates a table if it doesn't exist.

        Args:
            table (str): The name of the table to create.
            columns (list): A list of column names.
            types (list): A list of column types corresponding to the column names.
        """
        mydb = self.connect()
        mycursor = mydb.cursor()

        # Check if the table exists
        mycursor.execute(f"SHOW TABLES LIKE '{table}'")
        result = mycursor.fetchone()

        if not result:
            # Create the table if it doesn't exist
            column_definitions = ', '.join([f"{col} {typ}" for col, typ in zip(columns, types)])
            create_table_sql = f"CREATE TABLE {table} ({column_definitions})"
            print(create_table_sql)
            mycursor.execute(create_table_sql)
            print(f'[MYSQL] Table {table} created')
        else:
            print(f'[MYSQL] Table {table} already exists')

        mycursor.close()
        mydb.close()

    def write_list(self, table, columns, values):
        mydb = self.connect()
        mycursor = mydb.cursor()

        if isinstance(columns, str):
            column_names = f"({columns})"
            placeholders = '%s'
        else:
            column_names = f"({', '.join(columns)})"
            placeholders = ', '.join(['%s'] * len(columns))
            
        sql = f"INSERT INTO {table} {column_names} VALUES ({placeholders})"
        if len(values) > 1:
            mycursor.executemany(sql, values)
        else:
            mycursor.execute(sql, values[0])

        mydb.commit()
        print('[MYSQL]', mycursor.rowcount, "records added to database")
        mycursor.close()
        mydb.close()

    def get_table(self, table, column, sort_by=None, max_entries=None, descending=False, add_query=None):
        mydb = self.connect()
        mycursor = mydb.cursor()

        column_str = ', '.join(column) if isinstance(column, list) else column
        query = f"SELECT {column_str} FROM {table}"
        if sort_by:
            query += f" ORDER BY {sort_by}"
            if descending:
                query += " DESC"
        if max_entries:
            query += f" LIMIT {max_entries}"
        if add_query:
            query += " " + add_query
        query += ";"
        mycursor.execute(query)
        table_values = mycursor.fetchall()
        mycursor.close()
        mydb.close()
        return table_values
    
    def execute(self, query, fetch=False):
        mydb = self.connect()
        mycursor = mydb.cursor()
        mycursor.execute(query)
        
        if fetch:
            fetch = mycursor.fetchall()
            mydb.commit()
            mycursor.close()
            mydb.close()
            return fetch
        else:
            mydb.commit()
            mycursor.close()
            mydb.close()

    def get_dataframe(self, table, column, add_query=None):
        try:
            mydb = self.connect()
            query = f"SELECT {column} FROM {table}"
            if add_query:
                query += " " + add_query
            query += ";"
            print(query)
            result_df = pd.read_sql(query, mydb)
            mydb.close()
            return result_df
        except Exception as e:
            print(str(e))
            mydb.close()