#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# bug-report: makssych@gmail.com

"""Simple cli tool to find difference in data statistics.

cli tool
which work with Mysql(GC-WEB-replica), Mssql(GCCU) and Mongo(GC-WEB-replica).
Get data statistic for one day and send alert to telegram if find difference. 
"""
import logging
import logging.handlers
import os
import argparse
import sys
import json
import pyodbc
import ast
import requests
import MySQLdb as mdb
from pymongo import MongoClient
from datetime import datetime, date, timedelta



logger = logging.getLogger(os.path.splitext(os.path.basename(sys.argv[0]))[0])

def setup_logging(options):
    """Configure logging."""
    root = logging.getLogger("")
    root.setLevel(logging.WARNING)
    logger.setLevel(options.debug and logging.DEBUG or logging.INFO)
    if not options.silent:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(
            "%(levelname)s[%(name)s] %(message)s"))
        root.addHandler(ch)


class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                      argparse.ArgumentDefaultsHelpFormatter):
    pass


def parse_args(args=sys.argv[1:]):
    """Parse arguments."""
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    # MySQL CONNECT HELP
    g = parser.add_argument_group("mysql connect")
    g.add_argument("--mysqluser", metavar="N",
                   default="root",
                   type=str,
                   help="username for mysql connect")
    g.add_argument("--mysqlpass", metavar="N",
                   default="1234",
                   type=str,
                   help="password for mysql connect")
    g.add_argument("--mysqlhost", metavar="N",
                   default="127.0.0.1",
                   type=str,
                   help="host for mysql connect")
    g.add_argument("--mysqlport", metavar="N",
                   default="3306",
                   type=str,
                   help="port for mysql connect")
    # MSSQL CONNECT HELP
    g = parser.add_argument_group("mssql connect")
    g.add_argument("--msuser", metavar="N",
                   default="sa",
                   type=str,
                   help="username for mssql connect")
    g.add_argument("--mspass", metavar="N",
                   default="1234",
                   type=str,
                   help="password for mssql connect")
    g.add_argument("--mshost", metavar="N",
                   default="127.0.0.1",
                   type=str,
                   help="host for mssql connect")
    g.add_argument("--msport", metavar="N",
                   default="1143",
                   type=str,
                   help="port for mssql connect")
    # MONGO CONNECT HELP
    g = parser.add_argument_group("mongo connect")
    g.add_argument("--mongodburl", metavar="N",
                   default="mongodb://user:pass@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-1",
                   type=str,
                   help="url for mongodb connect")
    # SLACK HELP
    g = parser.add_argument_group("slack send message")
    g.add_argument("--useslack", action="store_true",
                   default=False,
                   help="set True for sending message to slack")
    g.add_argument("--slackwebhook", metavar="N",
                   default="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
                   type=str,
                   help="slack webhook for Your Workspace section")
    g.add_argument("--slackchanel", metavar="N",
                   default="#testing",
                   type=str,
                   help="slack chanel where post messages")
    # TELEGRAM HELP
    g = parser.add_argument_group("telegram send message")
    g.add_argument("--usetelegram", action="store_true",
                   default=False,
                   help="set True for sending message to telegram")
    g.add_argument("--telegramtoken", metavar="N",
                   default="",
                   type=str,
                   help="")
    g.add_argument("--telegramchatid", metavar="N",
                   default="",
                   type=str,
                   help="Chat id where send message")

    g = parser.add_mutually_exclusive_group()
    g.add_argument("--debug", "-d", action="store_true",
                   default=True,
                   help="enable debugging")
    g.add_argument("--silent", "-s", action="store_true",
                   default=False,
                   help="don't log to console")

    return parser.parse_args(args)


def date_yesterday():
    yesterday = date.today() - timedelta(days=1)
    return yesterday.strftime('%d')


def date_now():
    return datetime.now().strftime('%d')


def month_now():
    return datetime.now().strftime('%m')


def play_sufix():
    return datetime.now().strftime('%Y%m')


def ask_mssql(select, options):
    """ Make select from Mssql."""
    mssql_select = "select %s" % (select)
    logger.debug("Try to select from Mssql:\n {}".format(mssql_select))
    conn_string = 'DRIVER={{/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.4.so.1.1}}; SERVER={},{}; DATABASE=gccu; UID={}; PWD={}'.format(
            options.mshost, 
            options.msport,
            options.msuser,
            options.mspass
            )
    try:
        # Specifying the ODBC driver, server name, database, etc. directly
        # set DRIVER full path to msodbcsql 
        cnxn = pyodbc.connect(conn_string)
        
        # Create a cursor from the connection
        cursor = cnxn.cursor()
        cursor.execute(mssql_select)
        return cursor.fetchall()
        # for row in rows:
        #     print(row)
    except (pyodbc.ProgrammingError, pyodbc.IntegrityError) as e:
        message = "Error %d: %s" % (e.args[0],e.args[1])
        logger.debug(message)
        sys.exit(1)
    finally:
        if cnxn:
            cnxn.close()


def ask_mysql(select, options):
    """ Make select from Mysql."""
    mysql_select = "select %s" % (select)
    logger.debug("Try to select from MySQL:\n {}".format(mysql_select))
    try:
        con = mdb.connect(
            host = options.mysqlhost, 
            user = options.mysqluser, 
            passwd = options.mysqlpass,
            db = 'gold_cup_web', 
            port = int(options.mysqlport)
        )
        cur = con.cursor()
        cur.execute(mysql_select)
        return cur.fetchall()
        # for row in rows:
        #     print(row)
    except (mdb.Error, mdb.Warning) as e:
        message = "Error %d: %s" % (e.args[0],e.args[1])
        logger.debug(message)
        sys.exit(1)
    finally:
        if con:
            con.close()


def get_data_from_mongo_db(find_str, db):
    """ Make find in mongodb."""
    logger.debug("Try make find in MongoDB:\n {} in collection {}".format(find_str, db))
    try:
        # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
        client = MongoClient("mongodb://user:pass@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-1")
        #db=client.payment-db
        # Issue the serverStatus command and print the results
        q_count = client[db].pay_in_response.find(ast.literal_eval(find_str)).count()
        return q_count
    finally:
        if client:
            client.close()


def send_to_slack(slack_text, options):
    """ Send custom message to slack using webhook."""
    webhook_url = options.slackwebhook
    slack_data = {
        'text': '```{}```'.format(slack_text),
        'channel': options.slackchanel,
        'username': 'WEB-GC-BOT',
        'icon_emoji': ':robot_face:'
    }
    
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
    )


def send_to_telegram(telegram_text, options):
    """ Send custom message to telegram using bot_token."""
    bot_token = options.telegramtoken
    bot_chatID = options.telegramchatid
    send_text = "https://api.telegram.org/bot{}/sendMessage?chat_id={}&parse_mode=Markdown&text=```{}```".format(
        bot_token, 
        bot_chatID, 
        telegram_text
    )
    logger.debug(send_text)
    response = requests.get(send_text)
    if response.status_code != 200:
        raise ValueError(
            'Request to telegram returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
    )


def as_table_format(dict_text):
    """ Transform dict of tupels to text table."""
    text_head = "Found {} diff : \n".format(len(diff_items))
    text_body = "       METRIC       |  REPL_DATA |  GCCU_DATA \n " + "-"*47
    if dict_text:
        for key,value in dict_text.items():
            text_body += "\n {:^18} | {:^10} | {:^10}".format(key, value[0], value[1])
    return text_head + text_body



if __name__ == "__main__":
    options = parse_args()
    setup_logging(options)

    try:
        logger.info("Start plays_daily_statistics for {}".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        # Get info from MSSQL
        ms_select_from_plays = """
        {}EXAMPLE{}
        """.format(play_sufix(), date_yesterday())

        gccu_data_raw = ask_mssql(ms_select_from_plays, options)
        gccu_data = {}
        logger.debug("gccu_data: {}".format(gccu_data))
        # Get info from plays replica
        mysql_select_from_plays = """
        {}{}EXAMPLE{}{}
        """.format(gccu_data['play_min_id'], gccu_data['play_max_id'], date_yesterday(), month_now())

        repl_data_raw = ask_mysql(mysql_select_from_plays, options)
        repl_data = {}
        logger.debug("repl_data: {}".format(repl_data))
        # Get info from orders GCCU
        ms_select_from_orders = """
        
        """
        gccu_order_data_raw = ask_mssql(ms_select_from_orders, options)
        for row in gccu_order_data_raw:
            gccu_data[row[1]] = row[0]
            
        
        # Compare results
        x = repl_data
        y = gccu_data
        diff_items = {k: (x[k], y[k]) for k in x if k in y and x[k] != y[k]}
        text = as_table_format(diff_items)

        if not diff_items:
            text = "Everything is OK!"

        logger.debug(text)
        
        # Send mesage
        if options.useslack:
            send_to_slack(text, options)
        if options.usetelegram:
            send_to_telegram(text, options)

    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)
    sys.exit(0)
