""" 
    Name:           app.py
    Author:         Ryan Roberts
    Date:           May 9, 2021
    Description:    This module contains the LoadData class that connects to a SQL Server database
                    and executes the LOAD DATA INFILE utility to insert data from a CSV file into a table.
                    Includes a query function, that stores response as a CSV file.
                    Includes a function that sends the stored CSV file to an AWS S3 Bucket.

    Prerequisites:  1. Create the database data table.
                    2. Specify AWS S3 bucket name.
"""

import mysql.connector
import os
import boto3
import constants


class LoadData:
    def __init__(self, db_config: dict, table_name: str, file_path: str, s3_bucket: str,):
        self.db_config = db_config
        self.table_name = table_name
        self.file_path = file_path
        self.s3_bucket = s3_bucket

    # Load csv data into SQL table
    def load_data_into_db(self):
        db = mysql.connector.connect(**self.db_config)
        sql_query = f"LOAD DATA LOCAL INFILE '{self.file_path}' INTO TABLE {self.table_name};"
        try:
            curs = db.cursor()
            curs.execute(sql_query)
            db.commit()
            print('SQL EXECUTION COMPLETE')
            result_set = curs.fetchall()
            print('RESULT SET: ', result_set)
            return result_set
        except NameError:
            print('Error occurred', NameError)
            db.rollback()
        finally:
            db.close()

    # Query function
    def query(self):
        db = mysql.connector.connect(**self.db_config)
        sql_query = f'SELECT COUNT(DISTINCT location) from {self.table_name}'
        try:
            curs = db.cursor()
            curs.execute(sql_query)
            db.commit()
            print('SQL QUERY COMPLETE')
            result_set = curs.fetchall()
            print('result set', result_set)
        except NameError:
            print('Error occurred', NameError)
            db.rollback()
        finally:
            db.close()

    # Send to Amazon S3
    def send_to_aws(self, file_to_upload):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.upload_file(
                f'/tmp/{file_to_upload}', self.s3_bucket, file_to_upload)
        except:
            print('An error occurred')


# Example of creating an instance
DataObj = LoadData(constants.DB_CONFIG, './covid-data.csv',
                   'Main', 'city-hive-assignment')

# Example of using the instance
if __name__ in '__main__':
    DataObj.load_data_into_db()
