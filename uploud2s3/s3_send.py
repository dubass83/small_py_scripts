import logging
import logging.handlers
import os
import argparse
import sys
import requests
import json
import boto3
from botocore.exceptions import ClientError
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
    # SLACK HELP
    g = parser.add_argument_group('slack send message')
    g.add_argument(
        '--useslack', action='store_true',
        default=False,
        help='set True for sending message to slack',
    )
    g.add_argument(
        '--slackwebhook', metavar='N',
        default='https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX', # pragma: allowlist secret
        type=str,
        help='slack webhook for Your Workspace section',
    )
    g.add_argument(
        '--slackchanel', metavar='N',
        default='testing',
        type=str,
        help='slack chanel where post messages',
    )
    # TELEGRAM HELP
    g = parser.add_argument_group('telegram send message')
    g.add_argument(
        '--usetelegram', action='store_true',
        default=False,
        help='set True for sending message to telegram',
    )
    g.add_argument(
        '--telegramtoken', metavar='N',
        default='',
        type=str,
        help='',
    )
    g.add_argument(
        '--telegramchatid', metavar='N',
        default='-1001202079775',
        type=str,
        help='Chat id where send message, default it is 305',
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
    # S3 bucket HELP
    g = parser.add_argument_group('s3 bucket params')
    g.add_argument(
        '--bucket', metavar='s3-bucket',
        default='',
        type=str,
        help='set s3 bucket name for sending data',
    )

    return parser.parse_args(args)

def send_to_slack(slack_text, options):
    """ Send custom message to slack using webhook."""
    webhook_url = options.slackwebhook
    slack_data = {
        'text': '```{}```'.format(slack_text),
        'channel': '#{}'.format(options.slackchanel),
        'username': 'WEB-GC-BOT',
        'icon_emoji': ':robot_face:',
    }

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'},
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text),
        )


def send_to_telegram(telegram_text, options):
    """ Send custom message to telegram using bot_token."""
    bot_token = options.telegramtoken
    bot_chatID = options.telegramchatid
    send_text = 'https://api.telegram.org/bot{}/sendMessage?chat_id={}&parse_mode=Markdown&text=```{}```'.format(
        bot_token,
        bot_chatID,
        telegram_text,
    )
    #logger.debug(send_text)
    response = requests.get(send_text)
    if response.status_code != 200:
        raise ValueError(
            'Request to telegram returned an error %s, the response is:\n%s'
            % (response.status_code, response.text),
        )


def upload_file(file_name, options, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param options: Has atribute bucket to upload to and mesage config
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, options.bucket, object_name)
    except ClientError as e:
        logging.error(e)
        # Send mesage
        text = "Error sending file: {} to s3".format(file_name)
        if options.useslack:
            send_to_slack(text, options)
        if options.usetelegram:
            send_to_telegram(text, options)
        return False
    return True


def remove_file(full_file_name):
    """ Remove file from disk
    """
    if os.path.exists(full_file_name):
        os.remove(full_file_name)
    else:
        logger.error("The file: {} does not exist".format(full_file_name)) 


def get_list_files_from(dir):
    """ Get list of all files in directory
    """
    return os.listdir(dir)


def get_object_name(file_name):
    """ Get object name from file name and add it to file
    YYYY/MM/dd/file
    """
    return "{}/{}/{}/{}".format(file_name[4:8],file_name[8:10],file_name[10:12], file_name)


def main(options):
    logger.debug("Started upload csv files to s3")
    for file_name in get_list_files_from('/var/lib/mysql-files/'):
        object_name = get_object_name(file_name)
        logger.debug("object_name: {}".format(object_name))

        upload_file("/var/lib/mysql-files/{}".format(file_name), options, object_name)
        logger.debug("{} uplouded to s3".format(file_name))

        remove_file("/var/lib/mysql-files/{}".format(file_name))
    logger.debug("Fineshed upload csv file to s3")


if __name__ == '__main__':
    options = parse_args()
    setup_logging(options)
    main(options)
