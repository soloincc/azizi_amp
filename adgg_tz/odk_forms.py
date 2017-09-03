import requests, re, os
import logging, traceback, json
import copy
import subprocess
import hashlib
import dateutil.parser

from django.conf import settings
from ConfigParser import ConfigParser
from django.forms.models import model_to_dict
from datetime import datetime
from django.http import HttpResponse
from django.http import HttpRequest

from django.db import connection
from django.db import connections

from terminal_output import Terminal
from excel_writer import ExcelWriter
from models import ODKForm, RawSubmissions, FormViews, ViewsData, ViewTablesLookup, DictionaryItems, FormMappings
from sql import Query

terminal = Terminal()

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
logger = logging.getLogger('ODKForms')
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)
request = HttpRequest()


class OdkForms():
    def __init__(self):
        # self.server = 'http://odk.au-ibar.org/'
        self.server = settings.ONADATA_SETTINGS['HOST']
        self.ona_user = settings.ONADATA_SETTINGS['USER']
        self.ona_pass = settings.ONADATA_SETTINGS['PASSWORD']
        self.api_token = settings.ONADATA_SETTINGS['API_TOKEN']

        self.api_all_forms = 'api/v1/forms'
        self.form_data = 'api/v1/data/'
        self.form_stats = 'api/v1/stats/submissions/'
        self.form_rep = 'api/v1/forms/'
        self.media = 'api/v1/media'

        self.project = 'adgg'
        self.form_json_rep = None
        self.top_level_hierarchy = None
        self.cur_node_id = None
        self.form_group = None
        self.cur_form_id = None

        self.forms_settings = 'forms_settings.ini'
        self.form_connection = None
        self.country_qsts = ['c1s1q8_Country_name']
        self.clean_country_codes = None

        self.sub_counties = ['saku', 'laisamis', 'northhorr', 'moyale']

    def get_all_forms(self):
        """
        Get all the forms belonging to the current project
        """

        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})
        # check whether the form is already saved in the database
        try:
            all_forms = ODKForm.objects.all()
            for form in all_forms:
                to_return.append({'title': form.form_name, 'id': form.form_id})
        except Exception as e:
            terminal.tprint(str(e), 'fail')

        terminal.tprint(json.dumps(to_return), 'warn')
        return to_return

    def get_value_from_dictionary(self, t_key):
        query = """
            SELECT t_value from dictionary_items where t_key = '%s'
        """ % t_key
        with connection.cursor() as cursor:
            cursor.execute(query)
            t_value = cursor.fetchall()
            try:
                return t_value[0][0]
            except Exception as e:
                logging.error("Couldn't find the value for the key '%s' in the dictionary. %s" % (t_key, str(e)))
                terminal.tprint("Couldn't find the value for the key '%s' in the dictionary. %s" % (t_key, str(e)), 'fail')
                return "Unknown (%s)" % t_key

    def refresh_forms(self):
        """
        Refresh the list of forms in the database
        """
        url = "%s%s" % (self.server, self.api_all_forms)
        all_forms = self.process_curl_request(url)
        if all_forms is None:
            print "Error while executing the API request %s" % url
            return

        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})

        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        for form in all_forms:
            # check whether the form is already saved in the database
            try:
                saved_form = ODKForm.objects.get(full_form_id=form['id_string'])
                terminal.tprint("The form '%s' is already saved in the database" % saved_form.form_name, 'ok')
                to_return.append({'title': saved_form.form_name, 'id': saved_form.form_id})
            except ODKForm.DoesNotExist as e:
                # this form is not saved in the database, so save it
                terminal.tprint("The form '%s' is not in the database, saving it" % form['id_string'], 'warn')
                try:
                    cur_form_group = settings.get('id_' + str(form['formid']), 'form_group')
                except Exception as e:
                    cur_form_group = None

                cur_form = ODKForm(
                    form_id=form['formid'],
                    form_group=cur_form_group,
                    form_name=form['title'],
                    full_form_id=form['id_string'],
                    auto_update=False,
                    is_source_deleted=False
                )
                cur_form.publish()
                to_return.append({'title': form['title'], 'id': form['formid']})
            except Exception as e:
                terminal.tprint(str(e), 'fail')

        return to_return

    def get_all_submissions(self, form_id):
        """
        Given a form id, get all the submitted data
        """
        try:
            # the form_id used in odk_forms and submissions is totally different
            odk_form = ODKForm.objects.get(form_id=form_id)
            submissions = RawSubmissions.objects.filter(form_id=odk_form.id).values('raw_data')
            submitted_instances = self.online_submissions_count(form_id)

            # check whether all the submissions from the db match the online submissions
            if submitted_instances is None:
                # There was an error while fetching the submissions, use 0 as submitted_instances
                submitted_instances = 0

            terminal.tprint('\t%d -- %d' % (submissions.count(), submitted_instances), 'okblue')
            if submissions.count() == 0 and submitted_instances == 0:
                logger.info('There are no submissions to process')
                terminal.tprint('No submisions to process', 'fail')
                return None

            if submitted_instances > submissions.count():
                # we have some new submissions, so fetch them from the server and save them offline
                terminal.tprint("\tWe have some new submissions, so fetch them from the server and save them offline", 'info')
                # fetch the submissions and filter by submission time
                url = "%s%s%d.json?start=1&limit=5&sort=%s" % (self.server, self.form_data, form_id, '{"_submission_time":-1}')
                url = "%s%s%d.json?fields=[\"_uuid\", \"_id\"]" % (self.server, self.form_data, form_id)
                submission_uuids = self.process_curl_request(url)

                for uuid in submission_uuids:
                    # check if the current uuid is saved in the database
                    cur_submission = RawSubmissions.objects.filter(form_id=odk_form.id, uuid=uuid['_uuid'])
                    if cur_submission.count() == 0:
                        # the current submission is not saved in the database, so fetch and save it...
                        url = "%s%s%d/%s" % (self.server, self.form_data, form_id, uuid['_id'])
                        submission = self.process_curl_request(url)

                        t_submission = RawSubmissions(
                            form_id=odk_form.id,
                            # it seems some submissions don't have a uuid returned with the submission. Use our previous uuid
                            uuid=uuid['_uuid'],
                            submission_time=submission['_submission_time'],
                            raw_data=submission
                        )
                        t_submission.publish()
                    else:
                        # the current submission is already saved, so stop the processing
                        # terminal.tprint("The current submission is already saved, implying that all submissions have been processed, so stop the processing!", 'fail')
                        continue

                # just check if all is now ok
                submissions = RawSubmissions.objects.filter(form_id=odk_form.id).order_by('submission_time').values('raw_data')
                if submissions.count() != submitted_instances:
                    # ok, still the processing is not complete... shout!
                    terminal.tprint("Even after processing submitted responses for '%s', the tally doesn't match (%d vs %d)!" % (odk_form.form_name, submissions.count(), submitted_instances), 'error')
                else:
                    terminal.tprint("Submissions for '%s' successfully updated." % odk_form.form_name, 'info')
            else:
                terminal.tprint("All submissions for '%s' are already saved in the database" % odk_form.form_name, 'info')

        except Exception as e:
            logger.error('Some error....')
            logger.error(str(e))
            terminal.tprint(str(e), 'error')
            raise Exception(str(e))

        return submissions

    def online_submissions_count(self, form_id):
        # given a form id, process the number of submitted instances
        terminal.tprint("\tComputing the number of submissions of the form with id '%s'" % form_id, 'info')
        url = "%s%s%d?%s" % (self.server, self.form_stats, form_id, "group=&name=time")
        stats = self.process_curl_request(url)

        if stats is None:
            logger.error("Error while fetching the number of submissions")
            return None

        submissions_count = 0
        for stat in stats:
            submissions_count += int(stat['count'])

        return submissions_count

    def read_settings(self, settings_file, variable):
        parser = ConfigParser()
        parser.readfp(settings_file)

    def get_form_structure_as_json(self, form_id):
        """
        check whether the form structure is already saved in the DB
        """
        try:
            cur_form = ODKForm.objects.get(form_id=form_id)

            # check if the structure exists
            if cur_form.structure is None:
                # we don't have the structure, so fetch, process and save the structure
                terminal.tprint("\tThe form '%s' doesn't have a saved structure, so lets fetch it and add it" % cur_form.form_name, 'warn')
                (processed_nodes, structure) = self.get_form_structure_from_server(form_id)
                if structure is not None:
                    cur_form.structure = structure
                    cur_form.processed_structure = processed_nodes
                    cur_form.publish()
                else:
                    raise Exception("There was an error in fetching the selected form and it is not yet saved in the database.")
            else:
                terminal.tprint("\tFetching the form's '%s' structure from the database" % cur_form.form_name, 'okblue')
                processed_nodes = cur_form.processed_structure
                # terminal.tprint(json.dumps(cur_form.structure), 'okblue')
        except Exception as e:
            print(traceback.format_exc())
            logger.debug(str(e))
            terminal.tprint(str(e), 'fail')
            raise Exception(str(e))

        return processed_nodes

    def get_form_structure_from_server(self, form_id):
        """
        Get the structure of the current form
        """
        url = "%s%s%d/form.json" % (self.server, self.form_rep, form_id)
        terminal.tprint("Fetching the form structure for form with id = %d" % form_id, 'header')
        form_structure = self.process_curl_request(url)

        if form_structure is None:
            return (None, None)

        self.cur_node_id = 0
        self.cur_form_id = form_id
        self.repeat_level = 0
        self.all_nodes = []
        self.top_node = {"name": "Main", "label": "Top Level", "parent_id": -1, "type": "top_level", "id": 0}

        self.top_level_hierarchy = self.extract_repeating_groups(form_structure, 0)
        self.all_nodes.insert(0, self.top_node)
        terminal.tprint("Processed %d group nodes" % self.cur_node_id, 'warn')

        # print all the json for creating the tree
        # terminal.tprint(json.dumps(self.all_nodes), 'warn')
        return self.all_nodes, form_structure

    def extract_repeating_groups(self, nodes, parent_id):
        """
        Process a node and get the repeating groups
        """
        cur_node = []
        for node in nodes['children']:
            if 'type' in node:
                if 'label' in node:
                    node_label = self.process_node_label(node)
                else:
                    terminal.tprint("\t\t%s missing label. Using name('%s') instead" % (node['type'], node['name']), 'warn')
                    node_label = node['name']

                if node['type'] == 'repeat' or node['type'] == 'group':
                    terminal.tprint("\tProcessing %s" % node_label, 'okblue')
                    # only add a node when we are dealing with a repeat
                    if node['type'] == 'repeat':
                        self.cur_node_id += 1
                        t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name'], 'items': []}
                    else:
                        t_node = None

                    if 'children' in node:
                        terminal.tprint("\t%s-%s has %d children" % (node['type'], node_label, len(node['children'])), 'ok')
                        self.repeat_level += 1
                        # determine parent_id. If we are in a group, pass the current parent_id, else pass the cur_node_id
                        t_parent_id = self.cur_node_id if node['type'] == 'repeat' else parent_id
                        child_node = self.extract_repeating_groups(node, t_parent_id)

                        if len(child_node) != 0:
                            if t_node is None:
                                # we have something to save yet it wasn't wrapped in a repeat initially
                                # self.cur_node_id += 1
                                terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(child_node[0])), 'warn')
                                t_node = child_node[0]
                            else:
                                t_node['items'].append(child_node[0])
                    # else:
                        # this node has no children. If its a top level node, include it in the top level page
                    #    if self.repeat_level == 0:

                    if t_node is not None and node['type'] == 'repeat':
                        if 'items' in t_node and len(t_node['items']) == 0:
                            del t_node['items']
                        cur_node.append(t_node)
                        # terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(t_node)), 'warn')
                        self.add_to_all_nodes(t_node)
                else:
                    # before anything, add this node to the dictionary
                    if node['type'] != 'calculate':
                        self.add_dictionary_items(node, node['type'])

                    # if self.repeat_level == 0:
                    self.cur_node_id += 1
                    # terminal.tprint("\tAdding a top node child", 'ok')
                    t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name']}
                    self.all_nodes.append(t_node)
            else:
                # we possibly have the options, so add them to the dictionary
                self.add_dictionary_items(node, 'choice')

        self.repeat_level -= 1
        return cur_node

    def add_dictionary_items(self, node, node_type):
        # check if this key already exists
        dict_item = DictionaryItems.objects.filter(form_id=self.cur_form_id, t_key=node['name'])

        if dict_item.count() == 0:
            terminal.tprint(json.dumps(node), 'warn')
            node_label = node['label'] if 'label' in node else node['name']
            dict_item = DictionaryItems(
                form_id=self.cur_form_id,
                t_key=node['name'],
                t_type=node_type,
                t_value=node_label
            )
            dict_item.publish()

            if 'type' in node:
                if node['type'] == 'select one' or node['type'] == 'select all that apply':
                    if 'children' in node:
                        for child in node['children']:
                            self.add_dictionary_items(child, 'choice')

    def process_node_label(self, t_node):
        '''
        Process a label node and returns the proper label of the node
        '''
        node_type = self.determine_type(t_node['label'])
        if node_type == 'is_json':
            cur_label = t_node['label'][settings.DEFAULT_LOCALE]
        elif node_type == 'is_string':
            cur_label = t_node['label']
        else:
            raise Exception('Cannot determine the type of label that I have got! %s' % json.dumps(t_node['label']))

        return cur_label

    def add_to_all_nodes(self, t_node):
        # add a node to the list of all nodes for creating the tree
        if 'items' in t_node:
            del t_node['items']

        if 'label' in t_node:
            cur_label = self.process_node_label(t_node)
            t_node['label'] = cur_label
            if re.search(":$", cur_label) is not None:
                # in case the label was ommitted, use the name tag
                t_node['label'] = t_node['name']

        self.all_nodes.append(t_node)

    def initiate_form_database(self, form_name):
        self.form_connection = Query(form_name)
        self.form_connection.register_database()

        return False

    def delete_folder_contents(self, folder_path):
        """
        Given a path to a folder, delete its contents
        """
        for filename in os.listdir(folder_path):
            if filename == '.' or filename == '..':
                    continue
            terminal.tprint("Deleting '%s'" % folder_path + os.sep + filename, 'fail')
            os.unlink(folder_path + os.sep + filename)

    def save_user_view(self, form_id, view_name, nodes, all_submissions, structure):
        """
        Given a view with a section of the user defined data, create a view of the selected nodes
        """
        # get a proper view name
        prop_view_name = self.formulate_view_name(view_name)

        # save the submissions as an excel an then call a function to create the table(s)
        # create a temp dir for this
        if os.path.exists(prop_view_name):
            self.delete_folder_contents(prop_view_name)
        else:
            # create the directory
            terminal.tprint("Create the directory '%s'" % prop_view_name, 'warn')
            os.makedirs(prop_view_name)

        writer = ExcelWriter(prop_view_name, 'csv', prop_view_name)
        writer.create_workbook(all_submissions, structure)
        terminal.tprint("\tFinished creating the csv files", 'warn')

        # now we have all our selected submissions as csv files, so process them
        import_command = "csvsql --db 'postgresql:///%s?user=%s&password=%s' --encoding utf-8 --blanks --insert --tables %s %s"
        table_views = []
        for filename in os.listdir(prop_view_name):
            terminal.tprint(filename, 'fail')
            if filename == '.' or filename == '..':
                continue

            basename = os.path.splitext(filename)[0]
            table_name = "%s_%s" % (prop_view_name, basename)
            table_name_hash = hashlib.md5(table_name)
            terminal.tprint("Hashed the table name '%s'" % table_name, 'warn')
            table_name_hash_dig = "v_%s" % table_name_hash.hexdigest()
            print (table_name_hash_dig)
            terminal.tprint("Hashed the table name '%s' to '%s'" % (table_name, table_name_hash_dig), 'warn')

            filename = prop_view_name + os.sep + filename

            terminal.tprint("\tProcessing the file '%s' for saving to the database" % filename, 'okblue')
            if filename.endswith(".csv"):
                cmd = import_command % (
                    settings.DATABASES['default']['NAME'],
                    settings.DATABASES['default']['USER'],
                    settings.DATABASES['default']['PASSWORD'],
                    table_name_hash_dig,
                    filename,
                )
                terminal.tprint("\tRunning the command '%s'" % cmd, 'ok')
                print subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()

                # run commands to create primary key
                try:
                    with connection.cursor() as cursor:
                        logging.debug("Adding a primary ket constraint for the table '%s'" % table_name)
                        query = "alter table %s add primary key (%s)" % (table_name_hash_dig, 'unique_id')
                        cursor.execute(query)

                        # if table name has a main on it, it must have a _uuid field which should be unique
                        if re.search("main$", table_name) is not None:
                            # this is finicky, omit it for now
                            terminal.tprint("Not adding a unique constraint for column '_uuid'", 'fail')
                            # logging.debug("Adding unique constraint '%s' for the table '%s'" % ('_uuid', table_name))
                            # uquery = "alter table %s add constraint %s_%s unique (%s)" % (table_name_hash_dig, table_name_hash_dig, 'uuid', '_uuid')
                            # cursor.execute(uquery)
                        else:
                            # for the other tables, add an index to top_id
                            logging.debug("Adding indexes to '%s' and '%s' for the table '%s'" % ('top_id', 'parent_id', table_name))
                            uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'top_id', table_name_hash_dig, 'top_id')
                            cursor.execute(uquery)
                            uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'parent_id', table_name_hash_dig, 'parent_id')
                            cursor.execute(uquery)
                except Exception as e:
                    logging.error("For some reason can't create a primary key or unique key, raise an error and delete the view")
                    logging.error(str(e))
                    with connection.cursor() as cursor:
                        dquery = "drop table %s" % table_name_hash_dig
                        cursor.execute(dquery)
                    raise Exception("For some reason I can't create a primary key or unique key for the table %s. Deleting it entirely" % table_name)

                table_views.append({'table_name': table_name, 'hashed_name': table_name_hash_dig})

        # clean up process
        # delete the generated files
        self.delete_folder_contents(prop_view_name)
        os.rmdir(prop_view_name)

        form_view = FormViews.objects.filter(view_name=view_name)
        odk_form = ODKForm.objects.get(form_id=form_id)

        if form_view.count() == 0:
            # save the new view
            form_view = FormViews(
                form=odk_form,
                view_name=view_name,
                proper_view_name=prop_view_name,
                structure=nodes
            )
            form_view.publish()

            # save these submissions to the database
            for submission in all_submissions:
                new_submission = ViewsData(
                    view=form_view,
                    raw_data=submission
                )
                new_submission.publish()
        else:
            logger.error("Duplicate view name '%s'. Can't save." % view_name)
            # raise Exception("Duplicate view name '%s'. Can't save." % view_name)
            # return

        # add the tables to the lookup table of views
        for view in table_views:
            cur_view = ViewTablesLookup(
                view=form_view,
                table_name=view['table_name'],
                hashed_name=view['hashed_name']
            )
            cur_view.publish()

    def formulate_view_name(self, view_name):
        """
        Formulate a proper view name that will be used as the view name in the database
        """
        # convert all to lowercase
        view_name = view_name.lower()

        # convert non alpha numeric characters to spaces
        view_name = re.sub(r"[^a-zA-Z0-9]+", '_', view_name)
        form_group = re.sub(r"[^a-zA-Z0-9]+", '_', self.form_group)

        # create a unique view name
        view_name = "%s_%s" % (form_group, view_name)
        return view_name

    def formulate_db_name(self, form_name):
        # convert all to lowercase
        db_name = form_name.lower()
        db_name = db_name.replace('.', '_')
        return db_name

    def fetch_merge_data(self, form_id, nodes, d_format, download_type, view_name):
        """
        Given a form id and nodes of interest, get data from all associated forms
        """

        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        associated_forms = []
        try:
            # get all the form ids belonging to the same group
            self.form_group = settings.get('id_' + str(form_id), 'form_group')
            for section in settings.sections():
                this_group = settings.get(section, 'form_group')
                if this_group == self.form_group:
                    m = re.findall("/?id_(\d+)$", section)
                    associated_forms.append(m[0])
                else:
                    # form_group section doesn't exist, so skip this
                    logger.info("Not interested in this form (%s), so skip it" % this_group)
                    continue
            form_name = settings.get(self.form_group, 'name')
        except Exception as e:
            print(traceback.format_exc())
            # there is an error getting the associated forms, so get data from just one form
            terminal.tprint(str(e), 'fail')
            associated_forms.append(form_id)
            form_name = "Form%s" % str(form_id)
            logging.info(str(e))

        # having all the associated form ids, fetch the required data
        all_submissions = []

        # since we shall be merging similar forms as one, declare the indexes here
        self.cur_node_id = 0
        self.indexes = {}
        self.sections_of_interest = {}
        self.output_structure = {'main': ['unique_id']}
        self.indexes['main'] = 1

        for form_id in associated_forms:
            try:
                this_submissions = self.get_form_submissions_as_json(int(form_id), nodes)
            except Exception as e:
                logging.debug(traceback.format_exc())
                logging.error(str(e))
                terminal.tprint(str(e), 'fail')
                raise Exception(str(e))

            if this_submissions is None:
                continue
            else:
                terminal.tprint("\tTotal no of submissions %d" % len(this_submissions), 'warn')
                all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

        terminal.tprint("\tTotal no of submissions %d" % len(all_submissions), 'ok')
        if len(all_submissions) == 0:
            terminal.tprint("The form (%s) has no submissions for download" % str(form_name), 'fail')
            logging.debug("The form (%s) has no submissions for download" % str(form_name))
            return {'is_downloadable': False, 'error': False, 'message': "The form (%s) has no submissions for download" % str(form_name)}

        # check if there is need to create a database view of this data
        if download_type == 'download_save':
            try:
                self.save_user_view(form_id, view_name, nodes, all_submissions, self.output_structure)
            except Exception as e:
                return {'is_downloadable': False, 'error': True, 'message': str(e)}
        elif download_type == 'submissions':
            return all_submissions

        # now we have all the submissions, create the Excel sheet
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        if d_format == 'xlsx':
            # now lets save the data to an excel file
            output_name = './' + form_name + '_' + now + '.xlsx'
            self.save_submissions_as_excel(all_submissions, self.output_structure, output_name)
            return {'is_downloadable': True, 'filename': output_name}

    def save_submissions_as_excel(self, submissions, structure, filename):
        writer = ExcelWriter(filename)
        writer.create_workbook(submissions, structure)

    def get_form_submissions_as_json(self, form_id, screen_nodes):
        # given a form id get the form submissions
        # if the screen_nodes is given, process and return only the subset of data in those forms

        submissions_list = self.get_all_submissions(form_id)

        if submissions_list is None or submissions_list.count() == 0:
            terminal.tprint("The form with id '%s' has no submissions returning as such" % str(form_id), 'fail')
            return None

        print submissions_list.count()
        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        try:
            # get the fields to include as part of the form metadata
            form_meta = settings.get('id_' + str(form_id), 'metadata').split(',')
            self.pk_name = settings.get('id_' + str(form_id), 'pk_name')
            self.sk_format = settings.get('id_' + str(form_id), 'sk_name')
        except Exception as e:
            terminal.tprint("Form settings for form id (%d) haven't been defined" % form_id, 'fail')
            logger.info("The settings for the form id (%s) haven't been defined" % str(form_id))
            logger.debug(e)
            form_meta = []
            self.pk_name = 'hh_id'

        if screen_nodes is not None:
            screen_nodes.extend(form_meta)
            screen_nodes.append('unique_id')
        # terminal.tprint('\tPrint the selected nodes...', 'warn')
        # terminal.tprint(json.dumps(screen_nodes), 'warn')

        submissions = []
        i = 0
        for data in submissions_list:
            if i > 10:
                break
            # data, csv_files = self.post_data_processing(data)
            pk_key = self.pk_name + str(self.indexes['main'])
            if self.determine_type(data) == 'is_json':
                data = json.loads(data['raw_data'])
            data['unique_id'] = pk_key
            data = self.process_node(data, 'main', screen_nodes, False)

            submissions.append(data)
            self.indexes['main'] += 1
            i += 1

        return submissions

    def process_node(self, node, sheet_name, nodes_of_interest=None, add_top_id=True):
        # the sheet_name is the name of the sheet where the current data will be saved
        cur_node = {}

        for key, value in node.iteritems():
            # clean the key
            clean_key = self.clean_json_key(key)
            if clean_key == '_geolocation':
                continue

            # terminal.tprint("\t"+clean_key, 'okblue')
            if nodes_of_interest is not None:
                if clean_key not in nodes_of_interest:
                    continue

            # add this key to the sheet name
            if clean_key not in self.output_structure[sheet_name]:
                self.output_structure[sheet_name].append(clean_key)

            if clean_key in self.country_qsts:
                value = self.get_clean_country_code(value)

            is_json = True
            val_type = self.determine_type(value)

            if val_type == 'is_list':
                value = self.process_list(value, clean_key, node['unique_id'])
                is_json = False
            elif val_type == 'is_json':
                is_json = True
            elif val_type == 'is_zero':
                is_json = False
                value = 0
            elif val_type == 'is_none':
                terminal.tprint(key, 'warn')
                print value
                is_json = False
                value = 'N/A'
            else:
                is_json = False

            if is_json is True:
                node_value = self.process_node(value, clean_key, nodes_of_interest)
                cur_node[clean_key] = node_value
            else:
                node_value = value
                cur_node[clean_key] = value

            """
            if nodes_of_interest is not None:
                # at this point, we have our data, no need to check if we have the right key
                terminal.tprint("\tAdding the processed node (%s)" % clean_key, 'ok')
                if clean_key not in self.sections_of_interest:
                    self.sections_of_interest[clean_key] = []

                if isinstance(node_value, list):
                    for node_item in node_value:
                        self.sections_of_interest[clean_key].append(node_item)
                else:
                    self.sections_of_interest[clean_key].append(node_value)
            """
            if add_top_id is True:
                cur_node['top_id'] = self.pk_name + str(self.indexes['main'])

        return cur_node

    def determine_type(self, input):
        """
        determine the input from the user

        @todo, rely on the xls form to get the input type
        """
        try:
            float(input) + 2
        except Exception:
            if isinstance(input, list) is True:
                return 'is_list'
            elif input is None:
                return 'is_none'
            elif isinstance(input, dict) is True:
                return 'is_json'
            elif input == '0E-10':
                return 'is_zero'
            else:
                try:
                    json.loads(input)
                except ValueError:
                    if isinstance(input, basestring) is True:
                        return 'is_string'

                    terminal.tprint(str(input), 'fail')
                    return 'is_none'
                except Exception:
                    # try encoding the input as string
                    try:
                        json.loads(str(input))
                    except ValueError:
                        return 'is_json'
                    except Exception:
                        terminal.tprint(json.dumps(input), 'fail')
                        return 'is_none'
                    return 'is_json'
                return 'is_json'

        return 'is_int'

    def process_list(self, list, sheet_name, parent_key):
        # at times the input is a string and not necessary a json object

        # the sheet name is where to put this subset of data
        if sheet_name not in self.output_structure:
            self.output_structure[sheet_name] = ['unique_id', 'top_id', 'parent_id']
            self.indexes[sheet_name] = 1

        cur_list = []
        for node in list:
            val_type = self.determine_type(node)
            node['unique_id'] = sheet_name + '_' + str(self.indexes[sheet_name])

            if val_type == 'is_json':
                processed_node = self.process_node(node, sheet_name)
                processed_node['parent_id'] = parent_key
                cur_list.append(processed_node)
            elif val_type == 'is_list':
                processed_node = self.process_list(node, sheet_name, node['unique_id'])
                cur_list.append(processed_node)
            else:
                cur_list.append(node)

            self.indexes[sheet_name] += 1

        return cur_list

    def post_data_processing(self, data, csv_files):
        new_data = {}
        for key, node in data.iteritems():
            if isinstance(node, list) is True:
                if key not in csv_files:
                    csv_files[key] = []

        return (new_data, csv_files)

    def clean_json_key(self, j_key):
        # given a key from ona with data, get the sane(last) part of the key
        m = re.findall("/?(\w+)$", j_key)
        return m[0]

    def get_clean_country_code(self, code):
        if self.clean_country_codes is None:
            terminal.tprint('Adding the list of country codes', 'okblue')
            self.clean_country_codes = {}

            try:
                # get the country codes to clean
                settings = ConfigParser()
                settings.read(self.forms_settings)
                country_codes = settings.items('countries')
                for country, c_code in country_codes:
                    if re.search(",", c_code) is not None:
                        c_code = c_code.split(',')
                        for t_code in c_code:
                            self.clean_country_codes[t_code] = country
                    else:
                        self.clean_country_codes[c_code] = country

            except Exception as e:
                terminal.tprint(str(e), 'fail')
                return code

        # it seems we have our countries processed, just get the clean code
        try:
            if code in self.clean_country_codes:
                return self.clean_country_codes[code]
            else:
                for c_code, country in self.clean_country_codes.iteritems():
                    if re.search(c_code, code, re.IGNORECASE) is not None:
                        return country

                # if we are still here, the code wasnt found
                terminal.tprint("Couldn't find (%s) in the settings" % code, 'fail')
                terminal.tprint(c_code + '--' + country, 'okblue')
                print self.clean_country_codes
                print ''
                return code
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            return code

    def process_single_submission(self, node, watch_list):
        # given a node full of submission and a watchlist,
        # retrieve the datasets whose key is in the watchlist
        return node

    def process_curl_request(self, url):
        """
        Create and execute a curl request
        """
        headers = {'Authorization': "Token %s" % self.api_token}
        terminal.tprint("\tProcessing API request %s" % url, 'okblue')
        try:
            r = requests.get(url, headers=headers)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return None

        if r.status_code == 200:
            terminal.tprint("\tResponse %d" % r.status_code, 'ok')
            # terminal.tprint(json.dumps(r.json()), 'warn')
            return r.json()
        else:
            terminal.tprint("\tResponse %d" % r.status_code, 'fail')
            terminal.tprint(r.text, 'fail')
            terminal.tprint(url, 'warn')

            return None

    def get_views_info(self):
        form_views = FormViews.objects.all()

        all_data = {'views': []}
        for form_view in form_views:
            views_sub_table = ViewTablesLookup.objects.filter(view_id=form_view.id)
            view_date = form_view.date_created.strftime("%Y-%m-%d")
            all_data['views'].append({
                'view_id': form_view.id,
                'view_name': form_view.view_name,
                'date_created': view_date,
                'no_sub_tables': views_sub_table.count(),
                'auto_process': 'Yes'
            })
        return all_data

    def delete_view(self, request):
        view = json.loads(request.POST['view'])
        view_id = int(view['view_id'])
        try:
            # first delete the records in the views_table
            view_tables = ViewTablesLookup.objects.filter(view_id=view_id)
            for fview in view_tables:
                # delete the table
                logging.error("Drop the table '%s' in the view '%s'" % (fview.hashed_name, view['view_id']))
                with connection.cursor() as cursor:
                    # delete the actual view itself
                    dquery = "drop table %s" % fview.hashed_name
                    cursor.execute(dquery)
                # now delete the record
                fview.delete()

            # delete the view record in the database
            ViewsData.objects.filter(view_id=view_id).delete()
            FormViews.objects.filter(id=view_id).delete()
            return {'error': False, 'message': 'View deleted successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return {'error': True, 'message': str(e)}

    def edit_view(self, request):
        try:
            view = json.loads(request.POST['view'])
            # delete the actual view itself
            form_view = FormViews.objects.get(id=view['view_id'])
            form_view.view_name = view['view_name']
            # form_view.auto_process = view['auto_process']
            form_view.publish()

            return {'error': False, 'message': 'View edited successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            return {'error': True, 'message': str(e)}

    def proccess_submissions_count(self, s_count, use_zero=False):
        terminal.tprint(json.dumps(s_count), 'warn')

        to_return = 0
        if len(s_count) == 0:
            to_return = 0.001
        elif s_count[0][0] is None:
            to_return = 0.001
        else:
            to_return = int(s_count[0][0]) if len(s_count) != 0 else 0.001

        if to_return == 0.001:
            if use_zero is True:
                to_return = 0

        return to_return

    def save_mapping(self, request):
        '''
        @todo Add data validation
        '''
        data = json.loads(request.body)
        # get the form metadata
        settings = ConfigParser()
        settings.read(self.forms_settings)

        try:
            cur_form_group = settings.get('id_' + str(data['form']['id']), 'form_group')
        except Exception as e:
            return {'error': True, 'message': str(e)}

        mapping = FormMappings(
            form_group=cur_form_group,
            form_question=data['table_item']['name'],
            dest_table_name=data['table']['title'],
            dest_column_name=data['drop_item']['title'],
            odk_question_type=data['table_item']['type'],
            db_question_type=data['drop_item']['type']
        )
        mapping.publish()

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def mapping_info(self):
        all_mappings = FormMappings.objects.all().order_by('dest_table_name').order_by('dest_column_name')

        to_return = []
        for mapping in all_mappings:
            cur_mapping = model_to_dict(mapping)
            cur_mapping['_checkbox'] = '<input type="checkbox" class="row-checkbox">'
            cur_mapping['mapping_id'] = cur_mapping['id']
            to_return.append(cur_mapping)
        return to_return

    def clear_mappings(self):
        FormMappings.objects.all().delete()

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def delete_mapping(self, request):
        data = json.loads(request.POST['mappings'])
        FormMappings.objects.filter(id=data['mapping_id']).delete()

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def get_db_tables(self):
        with connections['mapped'].cursor() as cursor:
            tables_q = "SHOW tables"
            cursor.execute(tables_q)
            tables = cursor.fetchall()

            all_tables = []
            all_tables_columns = []
            all_tables.append({'title': 'Select One Table', 'id': '-1'})
            for parent_index, table in enumerate(tables):
                print "Processing %s" % table
                all_tables.append({'title': table[0], 'id': parent_index})
                columns_q = 'DESC %s' % table
                cursor.execute(columns_q)
                all_columns = cursor.fetchall()
                for index, col in enumerate(all_columns):
                    all_tables_columns.append({'title': col[0], 'type': col[1], 'id': index + 1000, 'parent_id': parent_index, 'label': '%s (%s)' % (col[0], col[1])})

        return all_tables, all_tables_columns

    def validate_mappings(self):
        '''
        Validate the mappings and ensure all mandatory fields have been mapped
        '''
        # get all the mapped tables
        mapped_tables = FormMappings.objects.values('dest_table_name').distinct()
        is_fully_mapped = True
        is_mapping_valid = True
        comments = []

        for table in mapped_tables:
            (is_table_fully_mapped, is_table_mapping_valid, table_comments) = self.validate_mapped_table(table['dest_table_name'])
            is_fully_mapped = is_fully_mapped and is_table_fully_mapped
            is_mapping_valid = is_mapping_valid and is_table_mapping_valid
            comments.extend(table_comments)

        return is_fully_mapped, is_mapping_valid, comments

    def validate_mapped_table(self, table):
        comments = []
        is_fully_mapped = True
        is_mapping_valid = True
        mapped_columns = FormMappings.objects.filter(dest_table_name=table)
        all_mapped_columns = {}
        for col in mapped_columns:
            all_mapped_columns[col.dest_column_name] = model_to_dict(col)

        with connections['mapped'].cursor() as mapped_cursor:
            dest_columns_q = 'DESC %s' % table
            mapped_cursor.execute(dest_columns_q)
            dest_columns = mapped_cursor.fetchall()

            # loop through all the destination columns and ensure mandatory fields have been mapped
            for dest_column in dest_columns:
                # check if the column in mandatory and is included in the mapping
                if dest_column[2] == 'NO':
                    # check if it is a primary key
                    if dest_column[3] == 'PRI' and dest_column[5] == 'auto_increment':
                        # its a primary key and auto incrementing, so skip it
                        continue
                    if dest_column[0] not in all_mapped_columns:
                        # check if we have a default value
                        if dest_column[4] is not None:
                            # we have a default value, so if it isn't mapped, we can safely ignore it
                            comments.append({'type': 'warning', 'message': "The column '%s' in the table '%s' is required but it is not mapped, I will use the defined default value." % (dest_column[0], table)})
                            continue
                        comments.append({'type': 'danger', 'message': "The column '%s' in the table '%s' requires a value but it is not mapped" % (dest_column[0], table)})
                        is_fully_mapped = False
                        continue

                # check the column data type
                # check the validation regex
                if dest_column[0] in all_mapped_columns:
                    if all_mapped_columns[dest_column[0]]['validation_regex'] is None:
                        comments.append({'type': 'warning', 'message': "Consider adding a validation regex for column '%s' of the table '%s'" % (dest_column[0], table)})

                # If the column is of type int, check if it a foreign key
                is_foreign_key = self.foreign_key_check(settings.DATABASES['mapped']['NAME'], table, dest_column[0])
                if is_foreign_key is not False and is_foreign_key[0] is not None:
                    # check that the corresponding table is fully mapped
                    (is_table_fully_mapped, is_table_mapping_valid, table_comments) = self.validate_mapped_table(is_foreign_key[1])
                    if not is_table_fully_mapped:
                        comments.append({'type': 'danger', 'message': "REFERENTIAL INTEGRITY FAIL: The referenced table '%s' is not fully mapped." % is_foreign_key[1]})
                        is_fully_mapped = False
                    if not is_table_mapping_valid:
                        comments.append({'type': 'danger', 'message': "REFERENTIAL INTEGRITY FAIL: The referenced table '%s' mapping is not valid." % is_foreign_key[1]})
                        is_mapping_valid = False

        return is_fully_mapped, is_mapping_valid, comments

    def foreign_key_check(self, schema, table, column):
        foreign_key_check_q = '''
            SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_NAME = '%s'
        ''' % (schema, table, column)

        with connections['mapped'].cursor() as mapped_cursor:
            mapped_cursor.execute(foreign_key_check_q)
            foreign_keys = mapped_cursor.fetchall()

            if len(foreign_keys) == 0:
                return False
            else:
                # we assume that the column is only mapped to only 1 other column
                return foreign_keys[0]

    def populateDestinationTables(self):
        # get all the destination tables from the destination schema and add them to the destination_tables table
        with connections['mapped'].cursor() as cursor:
            tables_q = "SHOW tables"
            cursor.execute(tables_q)
            tables = cursor.fetchall()

            for table in tables:
                dest_table = FormMappings(table_name=table)
                dest_table.publish()


def auto_process_submissions():
    terminal.tprint('Auto processing submissions', 'warn')
    odk_forms = OdkForms()
    all_submissions = odk_forms.fetch_merge_data(8, None, 'json', 'submissions', None)
    # terminal.tprint(json.dumps(all_submissions), 'ok')
    default_gps = "2.6460333333333335 36.92995166666666 1702.0 8.6"

    for subm in all_submissions:
        # check if the current submission is already processed
        s_inc = SyndromicIncidences.objects.filter(uuid=subm['_uuid'])
        if s_inc.count() == 1:
            terminal.tprint("Submission '%s' already processed, continue" % subm['uuid'], 'warn')
            continue
        else:
            # we have a submission to process
            datetime_subm = datetime.strptime(subm['_submission_time'], "%Y-%m-%dT%H:%M:%S")
            datetime_rep = datetime.strptime(subm['s0q2_start_time'][:23], "%Y-%m-%dT%H:%M:%S.%f")
            try:
                geo = subm['s1q1_gps'].split()
            except KeyError:
                geo = default_gps.split()
            # terminal.tprint(json.dumps(geo), 'warn')

            new_inc = SyndromicIncidences(
                uuid=subm['_uuid'],
                datetime_reported=datetime_rep.strftime("%Y-%m-%d %H:%M:%S"),
                datetime_uploaded=datetime_subm.strftime("%Y-%m-%d %H:%M:%S"),
                county=subm['s1q2_county'],
                sub_county=subm['s1q3_sub_county'],
                reporter=subm['s1q7_cdr_name'],
                latitude=geo[0],
                longitude=geo[1],
                accuracy=geo[3],
                no_cases=int(subm['s2q3_rpt_livestock_count'])
            )
            new_inc.publish()

            if subm['s2q1_new_cases'] == 'yes':
                top_inc = subm['s2q3_rpt_livestock'][0]
                for inc in subm['s2q3_rpt_livestock'][0]['s2q7_rpt_syndromes']:
                    terminal.tprint(json.dumps(inc), 'warn')
                    end_date = inc['s2q13_end_date'] if inc['s2q12_still_persistent'] == 'no' else None
                    inc_det = SyndromicDetails(
                        incidence=new_inc,
                        species=top_inc['s2q4_cur_livestock'],
                        syndrome=inc['s2q8_cur_syndrome'],
                        start_date=inc['s2q11_start_date'],
                        end_date=end_date,
                        herd_size=int(inc['s2q14_herd_size']),
                        no_sick=int(inc['s2q15_no_sick']),
                        no_dead=int(inc['s2q16_no_dead']),
                        clinical_signs=inc['s2q10_clinical_signs']
                    )
                    inc_det.publish()
