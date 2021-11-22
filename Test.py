import sqlite3
import json
from urllib.request import urlopen
from sqlite3 import Error


def load_data(address_url):
    response = urlopen(address_url)
    data = json.loads(response.read())
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


def main():
    url = "https://api.nbp.pl/api/exchangerates/tables/a/?format=json"
    database = 'nbpCurrencyRates.db'
    create_rates_table = """DROP TABLE IF EXISTS nbp_currency_rates;
                            CREATE TABLE nbp_currency_rates (
                                Currency text,
                                Code text,
                                Rate real);"""
    insert_rates_records = """INSERT INTO nbp_currency_rates (
                                currency, code, rate) 
                                VALUES (?, ?, ?)"""

    currency_rates = load_data(url)
    print(currency_rates)
    conn = create_connection(database)

    if conn is not None:
        create_table(conn, create_rates_table)
        insert_data(conn, currency_rates[0]['rates'], insert_rates_records,
                    'currency', 'code', 'mid')
        conn.commit()
    else:
        print("Error! Cannot create the database connection.")


if __name__ == '__main__':
    main()
