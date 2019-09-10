#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# bug-report: makssych@gmail.com

"""Simple cli tool to find difference in lots id.

cli tool
which work with Mysql(GC-WEB-replica) and Mssql(GCCU).
Return list of lots if find difference.
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
from datetime import datetime, date, timedelta



logger = logging.getLogger(os.path.splitext(os.path.basename(sys.argv[0]))[0])

def setup_logging(options):
    """Configure logging."""
    root = logging.getLogger('')
    root.setLevel(logging.WARNING)
    logger.setLevel(options.debug and logging.DEBUG or logging.INFO)
    if not options.silent:
        ch = logging.StreamHandler()
        ch.setFormatter(
            logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            ),
        )
        root.addHandler(ch)


class CustomFormatter(
    argparse.RawDescriptionHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter,
):
    pass


def parse_args(args=sys.argv[1:]):
    """Parse arguments."""
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter,
    )
    # Main config
    g = parser.add_argument(
        '--date', metavar='Date',
        type=str,
        help='Set data in format dd.mm.YYYY',
    )
    # MySQL CONNECT HELP
    g = parser.add_argument_group('mysql connect')
    g.add_argument(
        '--mysqluser', metavar='N',
        default='root',
        type=str,
        help='username for mysql connect',
    )
    g.add_argument(
        '--mysqlpass', metavar='N',
        default='1234',
        type=str,
        help='password for mysql connect',
    )
    g.add_argument(
        '--mysqlhost', metavar='N',
        default='127.0.0.1',
        type=str,
        help='host for mysql connect',
    )
    g.add_argument(
        '--mysqlport', metavar='N',
        default='3306',
        type=str,
        help='port for mysql connect',
    )
    # MSSQL CONNECT HELP
    g = parser.add_argument_group('mssql connect')
    g.add_argument(
        '--msuser', metavar='N',
        default='sa',
        type=str,
        help='username for mssql connect',
    )
    g.add_argument(
        '--mspass', metavar='N',
        default='1234',
        type=str,
        help='password for mssql connect',
    )
    g.add_argument(
        '--mshost', metavar='N',
        default='127.0.0.1',
        type=str,
        help='host for mssql connect',
    )
    g.add_argument(
        '--msport', metavar='N',
        default='1143',
        type=str,
        help='port for mssql connect',
    )

    g = parser.add_mutually_exclusive_group()
    g.add_argument(
        '--debug', '-d', action='store_true',
        default=True,
        help='enable debugging',
    )
    g.add_argument(
        '--silent', '-s', action='store_true',
        default=False,
        help="don't log to console",
    )

    return parser.parse_args(args)


def format_date(options):
    date_time_obj = datetime.strptime(options.date, '%d.%m.%Y')
    return date_time_obj.strftime('%d')


def format_month(options):
    date_time_obj = datetime.strptime(options.date, '%d.%m.%Y')
    return date_time_obj.strftime('%m')


def play_sufix(options):
    date_time_obj = datetime.strptime(options.date, '%d.%m.%Y')
    return date_time_obj.strftime('%Y%m')


def connect_mssql(options):
    """ Connect to Mssql and return cursor"""
    conn_string = 'DRIVER={{/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.4.so.1.1}}; SERVER={}; DATABASE=gccu; UID={}; PWD={}'.format(
            options.mshost,
            options.msuser,
            options.mspass,
    )
    try:
        # Specifying the ODBC driver, server name, database, etc. directly
        # set DRIVER full path to msodbcsql
        cnxn = pyodbc.connect(conn_string)

        # Create a cursor from the connection
        return cnxn.cursor()

    except (pyodbc.ProgrammingError, pyodbc.IntegrityError) as e:
        message = 'Error %d: %s' % (e.args[0],e.args[1])
        logger.debug(message)
        sys.exit(1)


def connect_mysql(options):
    """ Make connection to Mysql."""
    try:
        con = mdb.connect(
            host = options.mysqlhost,
            user = options.mysqluser,
            passwd = options.mysqlpass,
            db = 'gold_cup_web',
            port = int(options.mysqlport),
        )
        cur = con.cursor()
        return cur

    except (mdb.Error, mdb.Warning) as e:
        message = 'Error %d: %s' % (e.args[0],e.args[1])
        logger.debug(message)
        sys.exit(1)


def iter_row(cursor, size=10):
    while True:
        logger.debug('Start check {} rows from replica'.format(size))
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row


def main(options):
    """ Gets 'lots' from replica for the set date and check it one by one on GCCU.
    If not found 'lots' in GCCU then writing it to list. Return list in the end."""
    try:
        logger.info(
            'Start find diff lots id in replica and gccu for {}'.format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            ),
        )
        result = []
        # Get info from plays GCCU
                # Get info from plays GCCU
        ms_select_from_plays = """select min(p.transactionId) as 'minId',
        max(p.transactionId) as 'maxId',
        count(p.transactionId) as 'count'
        from {} p
        where day(p.createAt) = {}
        """.format(play_sufix(options), format_date(options))

        gccu_data_cursor = connect_mssql(options)
        gccu_data_cursor.execute(ms_select_from_plays)
        gccu_data_raw = gccu_data_cursor.fetchall()
        logger.debug(gccu_data_raw)
        gccu_data = {}
        gccu_data['play_min_id'] = gccu_data_raw[0][0]
        gccu_data['play_max_id'] = gccu_data_raw[0][1]
        gccu_data['count'] = int(gccu_data_raw[0][2])
        # gccu_data_cursor.close()
        logger.debug('gccu_data: {}'.format(gccu_data))

        # Get info from plays replica
        mysql_select_from_plays = """id
        from lots where id >= {} and
        id <= {} and
        state = 'SELL' and
        day(last_modified_date) = {} and
        month(last_modified_date) = {}
        """.format(
            gccu_data['play_min_id'],
            gccu_data['play_max_id'],
            format_date(options),
            format_month(options),
        )

        repl_data_cursor = connect_mysql(options)
        repl_data_cursor.execute('select {}'.format(mysql_select_from_plays))

        # run_test = 50

        for row in iter_row(repl_data_cursor, 100):
            # logger.debug('Get {} lots from REPLICA'.format(row[0]))
            ms_check_from_plays = """select count(p.transactionId)
            from {} p
            where day(p.createAt) = {} and
            p.transactionId = {}
            """.format(play_sufix(options), format_date(options), row[0])

            # logger.debug(ms_check_from_plays)
            gccu_data_cursor.execute(ms_check_from_plays)

            find_lot = gccu_data_cursor.fetchall()
            # logger.debug(find_lot)
            if not find_lot[0][0]:
                logger.debug('find diff for lot: {}'.format(row[0]))
                result.append(row[0])
            # run_test -= 1
            # if not run_test:
            #     break


        logger.info(
            'Completed find diffs in lots id in replica and gccu for {}'.format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            ),
        )

    except Exception as e:
        logger.exception('%s', e)
        sys.exit(1)

    finally:
        if gccu_data_cursor:
            gccu_data_cursor.close()
        if repl_data_cursor:
            repl_data_cursor.close()

    print(result)
    sys.exit(0)

if __name__ == '__main__':
    options = parse_args()
    setup_logging(options)
    main(options)
