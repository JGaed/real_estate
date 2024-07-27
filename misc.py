#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import mysql.connector
from config import mysql_host, mysql_password, mysql_user
import pandas as pd

class MySQL:
    """
    Represents a MySQL database interaction utility.

    Methods:
        connect(): Establishes a connection to the MySQL database.
        write_list(table, columns, values): Writes a list of values into the specified table.
        get_table(table, database): Retrieves data from a table in the specified database.
    """
    def connect():
        mydb = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database="real_estate"
        )
        return mydb

    def write_list(table, columns, values):
        mydb = MySQL.connect()
        mycursor = mydb.cursor()
        sql = "INSERT INTO {table} {columns} VALUES ({number_values})".format(
            table=table,
            columns=str(columns).replace("'", ""),
            number_values=', '.join(['%s' for x in range(len(columns))])
        )
        mycursor.executemany(sql, values)
        mydb.commit()
        print('[MYSQL]', mycursor.rowcount, "lines added to database")
        mycursor.close()
        mydb.close()

    def get_table(table, column, sort_by=None, max_entries=None):
        print(table, column, sort_by, max_entries)
        mydb = MySQL.connect()
        mycursor = mydb.cursor()
        query_str = ''
        if type(column)==str:
            query_str += ("SELECT {column} FROM {table}".format(column = column, table=table))
        if type(column)==list:
            query_str += ("SELECT {column} FROM {table}".format(column = ', '.join(column), table=table))
        if sort_by:
            query_str += (' ORDER BY {}'.format(sort_by))
        if max_entries:
            query_str += (' Limit {}'.format(str(max_entries)))
        print(query_str)
        mycursor.execute(query_str)
        table_values = mycursor.fetchall()    
        mycursor.close()
        mydb.close()
        return table_values
    
    def get_dataframe(table, column):
        try:
            mydb = MySQL.connect()
            query = "Select {column} from {table};".format(column = column, table=table)
            result_dataFrame = pd.read_sql(query,mydb)
            mydb.close() #close the connection
            return result_dataFrame
        except Exception as e:
            mydb.close()
            print(str(e))
        

def get_numbers(string_input):
    """
    Extracts and returns all numerical values from a string.

    Args:
        string_input (str): Input string containing numerical values.

    Returns:
        list: List of extracted numerical values.
    """
    return re.findall(r'\b\d+\b', string_input)

def get_lines(list_input, string_input):
    """
    Returns lines containing a specific string within a list and their corresponding line numbers.

    Args:
        list_input (list): Input list of strings.
        string_input (str): String to search for within the list.

    Returns:
        tuple: Tuple containing two lists: lines containing the specified string and their line numbers.
    """
    lines = [s for idx, s in enumerate(list_input) if string_input in s]
    lines_number = [idx for idx, s in enumerate(list_input) if string_input in s]
    return lines, lines_number

def chunkIt(seq, num):
    """
    Divides a sequence into approximately equal-sized chunks.

    Args:
        seq (list): Input sequence to be divided.
        num (int): Number of chunks to create.

    Returns:
        list: List of divided chunks.
    """
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def get_floats(string):
    numbers = []
    string = string.split()
    string = [x.replace(',','').replace('.','') for x in string]
    for x in string:
        try:
            numbers.append(float(x))
        except:
            None
        
    return numbers