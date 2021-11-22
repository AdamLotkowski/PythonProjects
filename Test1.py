import sqlite3
import json
from urllib.error import URLError
from urllib.request import urlopen
from sqlite3 import Error
from datetime import date, timedelta


def load_data(address_url):
    data = None
    try:
        response = urlopen(address_url)
        data = json.loads(response.read())
        return data
    except URLError as e:
        print(e)
    return data


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.executescript(create_table_sql)
        c.close()
    except Error as e:
        print(e)


def insert_data(conn, data, sql_insert_query, record1, record2, record3):
    try:
        c = conn.cursor()
        for i in data:
            c.execute(sql_insert_query, [i[record1], i[record2], i[record3]])
        c.close()
    except Error as e:
        print(e)


def main(from_date = (2021, 9, 18)):                             # data download start date

    database = 'nbpCurrencyRates.db'                             # sqlite3 database file
    conn = create_connection(database)                           # connecting with database


    print(from_date)

    dif = date.today() - date(*from_date)                        # difference between start date to today date
    dif_in_days = dif.days                                       # days difference in integer
    delta = 0
    while delta < dif_in_days:                                   # creating url addresses for each date
        delta += 1
        date_to_load = date.today() - timedelta(days=delta)
        date_to_url = date_to_load.strftime("%Y-%m-%d")
        url = "https://api.nbp.pl/api/exchangerates/tables/a/{}/?format=json".format(date_to_url)
        print(url)
        currency_rates = load_data(url)                          # download json file
        if currency_rates is not None:                           # if download success then save data to database

            date_to_insert = date_to_load.strftime("%Y_%m_%d")   # table name with correct date
            create_rates_table = """DROP TABLE IF EXISTS nbp"""+date_to_insert+""";
                                CREATE TABLE nbp"""+date_to_insert+""" (
                                    Currency text,
                                    Code text,
                                    Rate real);"""
            insert_rates_records = """INSERT INTO nbp"""+date_to_insert+""" (
                                    currency, code, rate) 
                                    VALUES (?, ?, ?)"""

            if conn is not None:
                create_table(conn, create_rates_table)
                insert_data(conn, currency_rates[0]['rates'], insert_rates_records,
                            'currency', 'code', 'mid')
            else:
                print("Error! Cannot create the database connection.")
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main((2021, 11,18))
