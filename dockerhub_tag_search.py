#!/usr/bin/env python3
import argparse
import ciso8601
import csv
import json
import humanfriendly
import math
import os
import re
import requests
import requests_cache
import sys
import tabulate
import time
import traceback
import urllib

from collections import defaultdict
from datetime import timedelta, datetime
from enum import Enum
from termcolor import colored
from typing import List


# ===== [ GLOBALS ] =====
VERBOSE = False
QUIET = False
EXAMPLE_USAGE = '''
Examples:
    {0} mysql
    {0} mysql --after 2016 --before 2017
    {0} nginx --after 2021 --operating-system linux --below 10M --architecture amd64
    {0} nginx -r '1\.2[0-3].*alpine.*perl.*'
    {0} httpd -n '*alpine*' --architecture '*arm64*'
    {0} python -s -r "3\.[67].*alpine.*" --architecture amd64
    {0} python -r "3\.(?:[89]|10).*alpine.*" --format json --architecture amd64 --operating-system linux | jq '.[] | .name + ";" + (.image_size|tostring) + ";" + (.image_size/1024/1024|floor|tostring) + "MB;" + .image_digest' | sort -t';' -k2
'''.format(os.path.basename(__file__))

# https://hub.docker.com/v2/repositories/<username>/<image>/tags?page_size=<N>&page=<N>
#   NOTE: <username> == library iff official image
#   NOTE: urlparse -> (scheme, netloc, url, params, query, fragment)
V2_API = urllib.parse.urlparse('https://hub.docker.com/v2/repositories/')
session = requests_cache.CachedSession(
    'session_cache.sqlite',
    use_cache_dir=True,
    expire_after=timedelta(days=1),
)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'X-DOCKER-API-CLIENT': 'docker-hub/1925.0.0',
    'Content-Type': 'application/json',
})

class Log_Type(Enum):
    """Types of logging"""
    FATAL_ERROR = 0x00
    ERROR       = 0x01
    WARNING     = 0x02
    INFORMATION = 0x03
    DEBUG       = 0x04

class Format(Enum):
    TABLE = 0
    JSON = 1
    CSV = 2

class ArgumentParserImplicitHelp(argparse.ArgumentParser):
    def error(self, message):
        self.print_help()
        print()
        log(message, Log_Type.ERROR)
        sys.exit(1)

class StatusCodeException(Exception):
    pass


# ===== [ FUNCTIONS ] =====
def log(message: str, log_type: Log_Type) -> None:
    if QUIET and log_type in [Log_Type.DEBUG, Log_Type.INFORMATION]:
        return

    if not VERBOSE and log_type == Log_Type.DEBUG:
        return

    colors = ['red', 'red', 'yellow', 'blue', 'white']
    custom_fmt = [
        '[FATAL ERROR]: {}', '[!]: {}', '[!]: {}', '[+]: {}', '[?]: {}'
    ]
    print(colored(custom_fmt[log_type.value].format(message), colors[log_type.value]))

def retrieve_tags_url(image_name: str, username: str, page=1, page_size=100) -> str:
    url = (
        V2_API.scheme,
        V2_API.netloc,
        urllib.parse.urljoin(V2_API.path, f'{username}/{image_name}/tags'),
        None,
        urllib.parse.urlencode({'page': page, 'page_size': page_size}),
        None
    )
    return urllib.parse.urlunparse(url)

def try_get(
        url: str,
        expected_status_code=None,
        retries=3,
        sleep=1
    ) -> requests.Response:
    while retries:
        retries -= 1
        try:
            log('TRYING TO GET {}'.format(url), Log_Type.DEBUG)
            response = session.get(url)
            if expected_status_code is not None and \
               response.status_code != expected_status_code:
               raise StatusCodeException

            log(('FROM CACHE ' if response.from_cache else 'GET ') + url,
                Log_Type.INFORMATION
            )
            return response
        except requests.RequestException:
            log(traceback.format_exc(), Log_Type.ERROR)
            if not retries:
                raise
        time.sleep(sleep)

def retrieve_tags(image_name: str, username: str, page_size=100) -> List[dict]:
    # NOTE:: dict_keys(['count', 'next', 'previous', 'results'])
    tags = []
    target_url = retrieve_tags_url(
        image_name=image_name,
        username=username,
        page=1,
        page_size=page_size
    )

    while target_url:
        try:
            response = try_get(target_url, expected_status_code=200)
        except StatusCodeException:
            log(
                'Possibly an invalid username or image name was given!\n' +\
                f'Username:   {username}\n' +\
                f'Image name: {image_name}',
                Log_Type.WARNING
            )
            return []
        tags_curr_page = json.loads(response.text)
        target_url = tags_curr_page['next']
        tags += tags_curr_page['results']
    return tags

def parse_args() -> argparse.Namespace:
    def wildcard_match_to_regex(pattern: str) -> str:
        if not pattern:
            return ''
        return '^' + '.*'.join(map(re.escape, pattern.split('*'))) + '$'

    global VERBOSE, QUIET
    parser = ArgumentParserImplicitHelp(
        epilog=EXAMPLE_USAGE,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        'image',
        help='The exact name of the image. Or <username>/<image>'
    )
    parser.add_argument(
        '-u',
        '--username',
        help='The name of the user who uploaded the given image'
    )
    parser.add_argument(
        '-n',
        '--name',
        default='',
        help='Case insensitive wildcard match the tags. (E.g.: 1.*alpine*)'
    )
    parser.add_argument(
        '-r',
        '--regex',
        default='',
        help='Case insensitive regex match the tags. '
    )
    parser.add_argument(
        '-a',
        '--architecture',
        default='',
        help='Search for the given CPU architecture. Supports wildcard search. \n'+
             'E.g.: amd64, arm32v5, arm32v6, arm32v7, arm64v8, i386, ppc64le, s390x, etc.'
    )
    parser.add_argument(
        '--operating-system',
        default='',
        help='Search for the given OS. Supports wildcard search\n'+\
             'E.g.: linux, windows, etc.'
    )
    parser.add_argument(
        '-s',
        '--sort',
        action='store_true',
        help='Sort by image size increasing order.'
    )
    parser.add_argument(
        '-f',
        '--format',
        choices=[x.name.lower() for x in Format],
        default=Format.TABLE.name,
        help='Specify the output format.'
    )
    parser.add_argument(
        '-b',
        '--below',
        default=None,
        help='Below the given size.'
    )
    parser.add_argument(
        '--after',
        default=-math.inf,
        help='After the given date.\n'+\
            'Example dates: ("2016", "2016-01", "2016-01-22", "2016-01-22 10", "2016-01-22 10:30:11")'
    )
    parser.add_argument(
        '--before',
        default=math.inf,
        help='Before the given date.\n'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Show DEBUG logs'
    )
    parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help='Suppress INFORMATION logs'
    )

    args = parser.parse_args()
    if args.image and '/' in args.image:
        temp = args.image.split('/')
        if args.username and temp[0] != args.username:
            raise ValueError(
                'You specified two different usernames: ' +\
                '[{}, {}]'.format(args.username, temp[0])
            )
        args.username, args.image = temp

    if args.regex and args.name:
        raise ValueError('Both regex and name is given')

    if args.name:
        args.regex = wildcard_match_to_regex(args.name)
    else:
        args.regex = args.regex

    args.operating_system = wildcard_match_to_regex(args.operating_system)
    args.architecture = wildcard_match_to_regex(args.architecture)

    if args.below:
        args.below = humanfriendly.parse_size(args.below)

    args.format = Format[args.format.upper()]
    if args.format in [Format.CSV, Format.JSON]:
        args.quiet = True

    if isinstance(args.before, str):
        if re.match('^[0-9]{4}$', args.before):
            args.before = args.before + '-01'
        args.before = time.mktime(ciso8601.parse_datetime(args.before).timetuple())
    if isinstance(args.after, str):
        if re.match('^[0-9]{4}$', args.after):
            args.after = args.after + '-01'
        args.after = time.mktime(ciso8601.parse_datetime(args.after).timetuple())

    VERBOSE = args.verbose
    QUIET = args.quiet
    if not args.username:
        args.username = 'library'
    return args

def get_image_os(tag: dict) -> str:
    os = tag['image_os'] or ''
    os += tag['image_os_features'] or ''
    os += tag['image_os_version'] or ''
    return os

def get_image_arch(tag: dict) -> str:
    arch = tag['image_architecture'] or ''
    arch += tag['image_features'] or ''
    arch += tag['image_variant'] or ''
    return arch

def filter_tags(tags: List[dict], pattern: str) -> List[dict]:
    if not pattern:
        return tags

    return [tag for tag in tags if
        re.match(pattern, tag['name'], re.IGNORECASE)]

def filter_arch(tags: List[dict], pattern: str) -> List[dict]:
    if not pattern:
        return tags

    return [tag for tag in tags if
        re.match(pattern, get_image_arch(tag), re.IGNORECASE)]

def filter_os(tags: List[dict], pattern: str) -> List[dict]:
    if not pattern:
        return tags

    return [tag for tag in tags if
        re.match(pattern, get_image_os(tag), re.IGNORECASE)]

def filter_date(tags: List[dict], after: float, before: float) -> List[dict]:
    if after == -math.inf and before == math.inf:
        return tags

    result = []
    for tag in tags:
        push_date: str = get_push_date_from_tag(tag)
        if push_date is None:
            continue

        push_date: datetime = ciso8601.parse_datetime(push_date)
        push_timestamp = time.mktime(push_date.timetuple())
        if after <= push_timestamp and push_timestamp <= before:
            result.append(tag)
    return result

def filter_size(tags: List[dict], size: int) -> List[dict]:
    if not size:
        return tags

    return [tag for tag in tags if
        tag['image_size'] <= size]

def expand_tags(tags: List[dict]) -> List[dict]:
    return [
        {
            **{k: v for k, v in tag.items() if k != 'images'},
            **dict(map(lambda x: ('image_'+x[0], x[1]), image.items()))
        }
        for tag in tags for image in tag['images']
    ]

def format_date(date: str, strf='%Y-%m-%d') -> str:
    if not date:
        return ''
    return ciso8601.parse_datetime(date).strftime(strf)

def get_push_date_from_tag(tag):
    # TODO:: What's the difference between image_last_pushed and tag_last_pushed?
    return tag['image_last_pushed'] if tag['image_last_pushed'] else tag['tag_last_pushed']

def table_print_tags(tags):
    print("")
    print(tabulate.tabulate(map(lambda tag: [
        tag['name'],
        get_image_os(tag),
        get_image_arch(tag),
        humanfriendly.format_size((tag['image_size'])),
        format_date(tag['image_last_pushed']),
        format_date(tag['tag_last_pushed']),
        '{}'.format(tag['image_size']),
        '{}..'.format(tag['image_digest'][:24]) if tag['image_digest'] else '',
        tag['image_status']
    ], tags), headers=[
        'Tag', 'OS', 'Arch', 'Size', 'Image pushed',
        'Tag pushed', 'Bytes', 'Digest', 'Image status'
    ], tablefmt='orgtbl'))

def csv_print_tags(tags):
    fields = tags[0].keys()
    stdout_writer = csv.DictWriter(sys.stdout, fieldnames=fields)
    stdout_writer.writeheader()
    for tag in tags:
        stdout_writer.writerow({k: (tag[k] if k in tag else None) for k in fields})

def get_number_of_tags_and_images(tags):
    n_tags = len(set(tag['name'] for tag in tags))
    n_images = len(tags)
    return n_tags, n_images

def main(args):
    tags = retrieve_tags(
        image_name=args.image,
        username=args.username,
        page_size=100
    )
    if not tags:
        return

    tags = list(map(lambda tag: defaultdict(lambda: None, tag), expand_tags(tags)))
    tags = filter_tags(tags, args.regex)
    tags = filter_arch(tags, args.architecture)
    tags = filter_os(tags, args.operating_system)
    tags = filter_date(tags, after=args.after, before=args.before)
    tags = filter_size(tags, size=args.below)
    if args.sort:
        tags = sorted(tags, key=lambda tag: int(tag['image_size']))

    n_tags, n_images = get_number_of_tags_and_images(tags)
    log('Number of tags:   {}'.format(n_tags), Log_Type.INFORMATION)
    log('Number of images: {}'.format(n_images), Log_Type.INFORMATION)

    if args.format == Format.TABLE:
        table_print_tags(tags)
    elif args.format == Format.JSON:
        print(json.dumps(tags))
    elif args.format == Format.CSV:
        csv_print_tags(tags)

if __name__ == '__main__':
    try:
        main(parse_args())
    except BrokenPipeError:
        pass
    finally:
        pass
