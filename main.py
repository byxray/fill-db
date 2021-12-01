import sqlite3, requests, re, schedule, time, os
from unicodedata import name
from sqlite3 import Error
from bs4 import BeautifulSoup
from datetime import datetime

UPDATE_TIME = int(os.getenv('UPDATE_TIME'))

banks_dict = {
    'absolute': {
        'url' : 'https://absolutbank.by/',
        'usd_buy_string' : 'body > div.b-wrapper > section.s-index-b.s-index-b--redesign > div > div.b-exchange-rate > div > div.b-exchange-rate__table > div > table > tbody > tr:nth-child(2) > td:nth-child(2) > span',
        'usd_sell_string' : 'body > div.b-wrapper > section.s-index-b.s-index-b--redesign > div > div.b-exchange-rate > div > div.b-exchange-rate__table > div > table > tbody > tr:nth-child(2) > td:nth-child(3) > span',
        'eur_buy_string' : 'body > div.b-wrapper > section.s-index-b.s-index-b--redesign > div > div.b-exchange-rate > div > div.b-exchange-rate__table > div > table > tbody > tr:nth-child(3) > td:nth-child(2) > span',
        'eur_sell_string' : 'body > div.b-wrapper > section.s-index-b.s-index-b--redesign > div > div.b-exchange-rate > div > div.b-exchange-rate__table > div > table > tbody > tr:nth-child(3) > td:nth-child(3) > span'
    },
    'fransabank': {
        'url' : 'https://fransabank.by/kursy-valyut/',
        'usd_buy_string' : '#officesList > table > tbody > tr:nth-child(1) > td.curr > div > div.currency_wrap > table > tbody > tr:nth-child(1) > td:nth-child(2)',
        'usd_sell_string' : '#officesList > table > tbody > tr:nth-child(1) > td.curr > div > div.currency_wrap > table > tbody > tr:nth-child(1) > td:nth-child(3)',
        'eur_buy_string' : '#officesList > table > tbody > tr:nth-child(1) > td.curr > div > div.currency_wrap > table > tbody > tr:nth-child(2) > td:nth-child(2)',
        'eur_sell_string' : '#officesList > table > tbody > tr:nth-child(1) > td.curr > div > div.currency_wrap > table > tbody > tr:nth-child(2) > td:nth-child(3)'
    },
    'paritet': {
        'url' : 'https://www.paritetbank.by/private/rates/?id_office=412',
        'usd_buy_string' : '#bx_3218110189_4537 > td:nth-child(2) > span',
        'usd_sell_string' : '#bx_3218110189_4537 > td:nth-child(3) > span',
        'eur_buy_string' : '#bx_3218110189_4538 > td:nth-child(2) > span',
        'eur_sell_string' : '#bx_3218110189_4538 > td:nth-child(3) > span'
    }
}

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def create_exchange_rate(conn, exchange_rate, bank):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = f''' INSERT INTO {bank}(usd_buy,usd_sell,eur_buy,eur_sell,address,date)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, exchange_rate)
    conn.commit()
    return cur.lastrowid

def cleanhtml(raw_html):
    cleantext = re.sub("[^0-9.]", '', raw_html)
    return cleantext

class Currency:
    def __init__(self, usd_buy, usd_sell, eur_buy, eur_sell):
        self.usd_buy = usd_buy
        self.usd_sell = usd_sell
        self.eur_buy = eur_buy
        self.eur_sell = eur_sell

def parse_bank(url, usd_buy_string, usd_sell_string, eur_buy_string, eur_sell_string):
    URL = url
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    page = requests.get(URL, headers=headers)

    soup = BeautifulSoup(page.content, 'html.parser')

    usd_buy = cleanhtml(str(soup.select(usd_buy_string)))
    usd_sell = cleanhtml(str(soup.select(usd_sell_string)))
    eur_buy = cleanhtml(str(soup.select(eur_buy_string)))
    eur_sell = cleanhtml(str(soup.select(eur_sell_string)))

    return Currency(usd_buy, usd_sell, eur_buy, eur_sell)

def get_date_time():

    now = datetime.now()

    global name_of_day
    name_of_day = now.strftime("%A")

    global hour
    hour = int(now.strftime("%H"))


def main():
    database = r"/usr/src/app/db/currency.db"


    # create a database connection
    conn = create_connection(database)
    with conn:

        time = datetime.now().replace(microsecond=0).isoformat()

        absolute_data = parse_bank(banks_dict["absolute"]["url"], banks_dict["absolute"]["usd_buy_string"], banks_dict["absolute"]["usd_sell_string"], banks_dict["absolute"]["eur_buy_string"], banks_dict["absolute"]["eur_sell_string"])
        exchange_rate_absolute = (absolute_data.usd_buy, absolute_data.usd_sell, absolute_data.eur_buy, absolute_data.eur_sell, "---", time)

        fransabank_data = parse_bank(banks_dict["fransabank"]["url"], banks_dict["fransabank"]["usd_buy_string"], banks_dict["fransabank"]["usd_sell_string"], banks_dict["fransabank"]["eur_buy_string"], banks_dict["fransabank"]["eur_sell_string"])
        exchange_rate_fransabank = (fransabank_data.usd_buy, fransabank_data.usd_sell, fransabank_data.eur_buy, fransabank_data.eur_sell, "---", time)

        paritet_data = parse_bank(banks_dict["paritet"]["url"], banks_dict["paritet"]["usd_buy_string"], banks_dict["paritet"]["usd_sell_string"], banks_dict["paritet"]["eur_buy_string"], banks_dict["paritet"]["eur_sell_string"])
        exchange_rate_paritet = (paritet_data.usd_buy, paritet_data.usd_sell, paritet_data.eur_buy, paritet_data.eur_sell, "---", time)

        # create exchange rate
        create_exchange_rate(conn, exchange_rate_absolute, 'absolute')
        create_exchange_rate(conn, exchange_rate_fransabank, 'fransabank')
        create_exchange_rate(conn, exchange_rate_paritet, 'paritet')

if __name__ == '__main__':
    schedule.every(UPDATE_TIME).minutes.do(main)

    while True :
        get_date_time()
        if name_of_day != "Sunday" and (hour > 8 and hour < 20) :
            schedule.run_pending()
            time.sleep(1)
        else:
            print("Relaxing...")
            time.sleep(3600)
            continue
