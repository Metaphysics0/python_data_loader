import mysql.connector

import constants


def query():
    db = mysql.connector.connect(**constants.DB_CONFIG)
    sql_query = f'SELECT COUNT(DISTINCT location) from Main'
    try:
        curs = db.cursor()
        curs.execute(sql_query)
        db.commit()
        print('SQL QUERY COMPLETE')
        result_set = curs.fetchall()
        print('result set', result_set)
    except NameError:
        print('Error incurred', NameError)
        db.rollback()
    finally:
        db.close()


if __name__ in '__main__':
    query()
