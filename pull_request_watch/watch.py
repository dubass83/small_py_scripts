#!/usr/bin/env python3
# coding: utf-8

import logging
import logging.handlers
import os
import argparse
import os
import sys
import requests
import json
import time
from datetime import datetime


INITIAL_MESSAGE = """\
Hi! <!here|here> there's a few open pull requests you should take a \
look at:
"""

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

    # Optional
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

    # Bitbucket HELP
    g = parser.add_argument_group('bitbucket params')
    g.add_argument(
        '--bitbucket', metavar='https://stash.example.net/',
        default='',
        type=str,
        help='set bitbucket url',
    )
    g.add_argument(
        '--project', metavar='GC',
        default='',
        type=str,
        help='set project key',
    )
    g.add_argument(
        '--repo', metavar='test-up',
        default='',
        type=str,
        help='set repo name',
    )
    g.add_argument(
        '--gituser', metavar='user',
        default='',
        type=str,
        help='set user name',
    )
    g.add_argument(
        '--gitpass', metavar='pass',
        default='',
        type=str,
        help='set user password',
    )
    return parser.parse_args(args)

def send_to_slack(slack_text, options):
    """ Send custom message to slack using webhook."""
    webhook_url = options.slackwebhook
    slack_data = {
        'text': '{}'.format(slack_text),
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


def format_pull_requests(json_obj):
    lines = []
    present = int(time.time())
    for i in json_obj['values']:
        created_on = int(i['createdDate'])//1000
        delta = round((present - created_on)/86400) 
        # send message if PR is older than 2 days
        logger.debug(
            "{}-created on: {}".format(
                i['title'],
                datetime.fromtimestamp(created_on)
            )
        )
        if delta > 2:
            try:
                line = '- <{1}|{2}> merge:{3} - by *{4}*\n\treviewer: *{5}*\n\tNo action for *{0}* day(s)\n\n'.format(delta,
                                                                          i['links']['self'][0]['href'],
                                                                          i['title'],
                                                                          i['properties']['mergeResult']['outcome'],
                                                                          i['author']['user']['slug'],
                                                                          i['reviewers'][0]['user']['slug']
                                                                          )
            except(KeyError):
                line = '- <{1}|{2}> merge:{3} - by @*{4}*\n\tNo action for *{0}* day(s)\n\n'.format(delta,
                                                                          i['links']['self'][0]['href'],
                                                                          i['title'],
                                                                          i['properties']['mergeResult']['outcome'],
                                                                          i['author']['user']['slug']
                                                                          )          
            lines.append(line)
    return lines


def fetch_pulls(options):

    lines = []  

    """
    Returns a formatted string list of open pull request messages.
    """
    response = requests.get(
        "{}rest/api/1.0/projects/{}/repos/{}/pull-requests".format(options.bitbucket, options.project, options.repo),
        auth=(options.gituser, options.gitpass)
        )

    if(response.ok):
        json_obj = json.loads(response.content)
        lines += format_pull_requests(json_obj)

    return lines


def main(options):
    lines = fetch_pulls(options)
    if lines:
        REPO_TEXT = '\n' + '#'*40 + '\n\t' + 'Project: ' + options.project + '\n\tRepo: ' + options.repo + '\n' + '#'*40 + '\n\n'
        text = INITIAL_MESSAGE + REPO_TEXT + '\n'.join(lines)
        if options.useslack:
            send_to_slack(text, options)
        logger.debug(text)


if __name__ == '__main__':
    options = parse_args()
    setup_logging(options)
    main(options)
