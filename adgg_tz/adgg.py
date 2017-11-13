import requests, re, os, collections
import logging, traceback, json
import copy
import subprocess
import hashlib
import dateutil.parser
import math

from ConfigParser import ConfigParser
from datetime import datetime
from collections import defaultdict, OrderedDict

from django.conf import settings
from django.forms.models import model_to_dict
from django.db import connection, connections, transaction, IntegrityError
from django.core.paginator import Paginator
from django.http import HttpResponse, HttpRequest
from raven import Client

from terminal_output import Terminal
from excel_writer import ExcelWriter
from models import ODKForm, RawSubmissions, FormViews, ViewsData, ViewTablesLookup, DictionaryItems, FormMappings, ProcessingErrors, ODKFormGroup
from sql import Query

terminal = Terminal()
sentry = Client('http://412f07efec7d461cbcdaf686c3b01e51:c684fccd436e46169c71f8c841ed3b00@sentry.badili.co.ke/3')

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
        },
    },
}
logger = logging.getLogger('ADGG')
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)
request = HttpRequest()


class ADGG():
    def __init__(self):
        return

    def system_stats(self):
        stats = defaultdict(dict)
        stats['farmers'] = defaultdict(dict)

        with connections['mapped'].cursor() as cursor:
            # get the number of processed farmers
            farmers_q = "SELECT count(*) as count from households"
            cursor.execute(farmers_q)
            farmers = cursor.fetchone()
            if farmers is None:
                raise AssertionError('There was some error while fetching data from the database')

            stats['farmers']['count'] = farmers[0]

        return stats
