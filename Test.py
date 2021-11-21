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
        c.execute(create_table_sql)
        c.close()
    except Error as e:
        print(e)


def insert_data(conn, sql_insert_query, records):
    try:
        c = conn.cursor()
        c.executemany(sql_insert_query, records)
        c.close()
    except Error as e:      
        print(e)


def main():
    url = "https://api.nbp.pl/api/exchangerates/tables/a/?format=json"
    database = 'nbpCurrencyRates.db'

    create_rates_table = """CREATE TABLE IF NOT EXISTS nbp_currency_rates (
                                currency text NOT NULL,
                                code text NOT NULL,
                                rate real NOT NULL
                                );"""

    insert_rates_records = """INSERT INTO nbp_currency_rates (
                                currency, code, rate) 
                                VALUES (?, ?, ?
                                );"""

 #   currency_rates = load_data(url)
  #  print(currency_rates[0]['rates'][0])

    recordsToInsert = [('Jos', 'jos@gmail.com', 9500),
                   ('Chris', 'chris@gmail.com', 7600),
                   ('Jonny', 'jonny@gmail.com', 8400)]

    conn = create_connection(database)

    if conn is not None:
#        create_table(conn, create_rates_table)
        insert_data(conn, insert_rates_records, recordsToInsert)
        conn.commit()
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()


##      import currency rates from NBP in JSON file
##      from urllib.request import urlopen
##      import json
##
##      url = "https://api.nbp.pl/api/exchangerates/tables/a/?format=json"
##      response = urlopen(url)
##      currencyRates = json.loads(response.read())
##
##      print(currencyRates[0]['rates'][0])
##
##
##      # import pprint                       # to erase
##      # pprint.pprint(currencyRates)            # to erase
##
##
##      # conn.close()
##