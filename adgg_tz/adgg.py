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

from .odk_forms import OdkForms

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

    def fetch_processing_status(self, cur_page, per_page, offset, sorts, queries):
        """
        Fetch the processing status of all the forms
        @todo: Proper pagination of the results
        Args:
            cur_page (TYPE): Description
            per_page (TYPE): Description
            offset (TYPE): Description
            sorts (TYPE): Description
            queries (TYPE): Description
        Returns:
            array: Returns an array with the processing status and a JSON of the form status
        """
        with connection.cursor() as cursor:
            form_details_q = 'SELECT b.form_id, b.form_name, c.group_name, a.is_processed, count(*) as r_count FROM raw_submissions as a INNER JOIN odkform as b on a.form_id=b.id INNER JOIN form_groups as c on b.form_group_id=c.id GROUP BY b.id, a.is_processed ORDER BY c.group_name, b.form_id, a.is_processed'
            cursor.execute(form_details_q)
            form_details = self.dictfetchall(cursor)

            to_return = {}
            for res in form_details:
                if res['form_id'] not in to_return:
                    to_return[res['form_id']] = {
                        'form_id': res['form_id'],
                        'form_name': res['form_name'],
                        'form_group': res['group_name'],
                        'no_submissions': 0,
                        'no_processed': res['r_count'] if res['is_processed'] == 1 else 0,
                        'unprocessed': res['r_count'] if res['is_processed'] == 0 else 0
                    }
                else:
                    if res['is_processed'] == 1:
                        to_return[res['form_id']]['no_processed'] += res['r_count']
                    elif res['is_processed'] == 0:
                        to_return[res['form_id']]['unprocessed'] += res['r_count']

                to_return[res['form_id']]['no_submissions'] += res['r_count']

        return_this = []
        for form_id, details in to_return.iteritems():
            # "{0:0.2f}".format(loc[1])
            # details['perc_error'] = "{:.2f}".format(((details['no_submissions'] - details['no_processed']) / details['no_submissions']) * 100)
            # details['perc_error'] = int(details['unprocessed']) / int(details['no_submissions'])
            details['perc_error'] = details['unprocessed']
            # details['perc_error'] = 3
            return_this.append(details)

        return False, {'records': return_this, "queryRecordCount": len(return_this), "totalRecordCount": len(return_this)}

    def system_stats(self):
        """Get the system summary
        Returns:
            json: A JSON with the system summary
        Raises:
            AssertionError: Incase of empty values, raises an assertion error
        """
        stats = defaultdict(dict)
        stats['farmers'] = defaultdict(dict)
        stats['animals'] = defaultdict(dict)
        stats['formgroups'] = defaultdict(dict)

        with connections['mapped'].cursor() as cursor:
            # get the number of processed farmers
            farmers_q = "SELECT count(*) as count from households"
            cursor.execute(farmers_q)
            farmers = cursor.fetchone()
            if farmers is None:
                raise AssertionError('There was some error while fetching data from the database')

            # get the gender of the household heads
            farmers_gender_q = "SELECT b.t_value, count(*) FROM `hh_attributes` as a inner join dictionary_items as b on a.attribute_type_id=b.id where b.form_group = 'farmer_reg' and parent_node = 'farmergender' group by attribute_type_id"
            cursor.execute(farmers_gender_q)
            farmers_gender = cursor.fetchall()
            if farmers_gender is None:
                raise AssertionError('There was some error while fetching data from the database')

            by_gender = []
            for f_gender in farmers_gender:
                by_gender.append({'name': f_gender[0], 'y': f_gender[1]})

            # get the number of processed animals
            animals_q = "SELECT count(*) as count from animals"
            cursor.execute(animals_q)
            animals = cursor.fetchone()
            if animals is None:
                raise AssertionError('There was some error while fetching data from the database')

            animals_sex_q = "SELECT sex, count(*) from animals group by sex"
            cursor.execute(animals_sex_q)
            animals_sex = cursor.fetchall()
            if animals_sex is None:
                raise AssertionError('There was some error while fetching data from the database')

            by_sex = []
            for a_sex in animals_sex:
                by_sex.append({'name': a_sex[0], 'y': a_sex[1]})

            # get the formgroup processing status
            formgroup_status = self.formgroup_processing_status()

            stats['farmers']['count'] = farmers[0]
            stats['farmers']['by_gender'] = by_gender
            stats['animals']['count'] = animals[0]
            stats['animals']['by_sex'] = by_sex
            stats['formgroups']['processing_status'] = formgroup_status

        return stats

    def formgroup_processing_status(self):
        with connection.cursor() as cursor:
            form_details_q = 'SELECT c.group_name, a.is_processed, count(*) as r_count FROM raw_submissions as a INNER JOIN odkform as b on a.form_id=b.id INNER JOIN form_groups as c on b.form_group_id=c.id GROUP BY c.group_name, a.is_processed ORDER BY c.group_name, b.form_id, a.is_processed'
            cursor.execute(form_details_q)
            odk = OdkForms()
            form_details = odk.dictfetchall(cursor)

            processing_status = defaultdict(dict)
            for res in form_details:
                if res['group_name'] not in processing_status:
                    processing_status[res['group_name']] = []
                    processing_status[res['group_name']].append({
                        'y': res['r_count'],
                        'name': 'Processed' if res['is_processed'] == 1 else 'Unprocesssed'
                    })
                else:
                    processing_status[res['group_name']].append({
                        'y': res['r_count'],
                        'name': 'Processed' if res['is_processed'] == 1 else 'Unprocesssed'
                    })

        return processing_status
