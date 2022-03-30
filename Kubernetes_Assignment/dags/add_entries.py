import psycopg2
import logging
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s: %(levelname)s --> %(funcName)s() --> %(message)s')
def insert_data():

    try:
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()
        table = '''CREATE TABLE if not exists execution_time_table(DAG_ID varchar(250), Execution_Date TIMESTAMPTZ);'''
        cursor.execute(table)
        insert = """insert into execution_time_table(DAG_ID, Execution_Date)
        select DAG_ID, Execution_Date from dag_run order by Execution_Date desc limit 1;"""
        cursor.execute(insert)
        conn.commit()
        logging.info("Data Insertion Successful")
    except Exception as e:
        logging.info("Error in connection",e)
    finally:
        conn.close()