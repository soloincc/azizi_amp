from __future__ import absolute_import  # Python 2 only

import json
import logging
import traceback
import gzip
import posixpath

from urlparse import unquote
from cStringIO import StringIO as IO

from django.conf import settings
from django.contrib.staticfiles import finders
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.views import static
from django.http import HttpResponse, Http404
from django.shortcuts import render, redirect
from django.middleware import csrf

from wsgiref.util import FileWrapper

from .odk_forms import OdkForms
from .terminal_output import Terminal

import os
terminal = Terminal()


def login_page(request):
    csrf_token = get_or_create_csrf_token(request)
    page_settings = {'page_title': "%s | Login Page" % settings.SITE_NAME, 'csrf_token': csrf_token}
    terminal.tprint(csrf_token, 'ok')

    try:
        username = request.POST['username']
        password = request.POST['pass']

        if username is not None:
            user = authenticate(username=username, password=password)

            if user is None:
                terminal.tprint("Couldn't authenticate the user... redirect to login page", 'fail')
                page_settings['error'] = 'Invalid username or password'
                return render(request, 'login.html', page_settings)
            else:
                terminal.tprint('All ok', 'debug')
                login(request, user)
                return redirect('/dashboard', request=request)
        else:
            return render(request, 'login.html')
    except KeyError as e:
        # ask the user to enter the username and/or password
        terminal.tprint('Username/password not defined', 'warn')
        page_settings['message'] = "Please enter your username and password"
        return render(request, 'login.html', page_settings)
    except Exception as e:
        terminal.tprint(str(e), 'fail')
        page_settings['error'] = "There was an error while authenticating.<br />Please try again and if the error persist, please contact the system administrator"
        return render(request, 'login.html', page_settings)


def logout_view(request):
    logout(request)
    return render(request, 'landing.html')


def under_review_page(request):
    return render(request, 'under_review.html')


def landing_page(request):
    get_or_create_csrf_token(request)
    return render(request, 'landing.html')


# @login_required(login_url='/login')
def download_page(request):
    csrf_token = get_or_create_csrf_token(request)

    # get all the data to be used to construct the tree
    odk = OdkForms()
    all_forms = odk.get_all_forms()
    page_settings = {
        'page_title': "%s | Downloads" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Download Section',
        'all_forms': json.dumps(all_forms)
    }
    return render(request, 'download.html', page_settings)


# @login_required(login_url='/login')
def modify_view(request):

    odk = OdkForms()
    if (request.get_full_path() == '/edit_view/'):
        response = odk.edit_view(request)
    elif (request.get_full_path() == '/delete_view/'):
        response = odk.delete_view(request)

    return HttpResponse(json.dumps(response))


# @login_required(login_url='/login')
def manage_views(request):
    csrf_token = get_or_create_csrf_token(request)

    # get all the data to be used to construct the tree
    odk = OdkForms()
    all_data = odk.get_views_info()

    page_settings = {
        'page_title': "%s | Manage Generated Views" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Manage Views',
        'all_data': json.dumps(all_data)
    }
    return render(request, 'manage_views.html', page_settings)


# @login_required(login_url='/login')
def update_db(request):
    odk = OdkForms()

    try:
        odk.update_sdss_db()
    except Exception as e:
        logging.error(traceback.format_exc())
        print str(e)
        return HttpResponse(traceback.format_exc())

    return HttpResponse(json.dumps({'error': False, 'message': 'Database updated'}))


def show_dashboard(request):
    csrf_token = get_or_create_csrf_token(request)

    odk = OdkForms()
    stats = odk.system_stats()

    page_settings = {
        'page_title': "%s | Home" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'ADGG Overview',
        'data': stats
    }
    return render(request, 'dash_home.html', page_settings)


# @login_required(login_url='/login')
def form_structure(request):
    # given a form id, get the structure for the form
    odk = OdkForms()
    try:
        form_id = int(request.POST['form_id'])
        structure = odk.get_form_structure_as_json(form_id)
    except KeyError as e:
        logging.error(traceback.format_exc())
        return HttpResponse(json.dumps({'error': True, 'message': str(e)}))
    except Exception as e:
        logging.info(str(e))
        logging.debug(traceback.format_exc())
        return HttpResponse(json.dumps({'error': True, 'message': str(e)}))

    return HttpResponse(json.dumps({'error': False, 'structure': structure}))


# @login_required(login_url='/login')
def download_data(request):
    # given the nodes, download the associated data
    odk = OdkForms()
    try:
        data = json.loads(request.body)
        res = odk.fetch_merge_data(data['form_id'], data['nodes[]'], data['format'], data['action'], data['view_name'])
    except KeyError as e:
        response = HttpResponse(json.dumps({'error': True, 'message': str(e)}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': True, 'message': str(e)})
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        logging.error(str(e))
        response = HttpResponse(json.dumps({'error': True, 'message': str(e)}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': True, 'message': str(e)})
        return response

    if res['is_downloadable'] is True:
        filename = res['filename']
        wrapper = FileWrapper(file(filename))
        response = HttpResponse(wrapper, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % os.path.basename(filename)
        response['Content-Length'] = os.path.getsize(filename)
    else:
        response = HttpResponse(json.dumps({'error': False, 'message': res['message']}), content_type='text/json')
        response['Content-Message'] = json.dumps({'error': False, 'message': res['message']})

    os.remove(filename)
    return response


# @login_required(login_url='/login')
def download(request):
    # given the nodes, download the associated data
    odk = OdkForms()
    try:
        data = json.loads(request.body)
        filename = odk.fetch_data(data['form_id'], data['nodes[]'], data['format'])
    except KeyError:
        return HttpResponse(traceback.format_exc())
    except Exception as e:
        print str(e)
        logging.error(traceback.format_exc())

    wrapper = FileWrapper(file(filename))
    response = HttpResponse(wrapper, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=%s' % os.path.basename(filename)
    response['Content-Length'] = os.path.getsize(filename)

    return response


# @login_required(login_url='/login')
def refresh_forms(request):
    """
    Refresh the database with any new forms
    """
    odk = OdkForms()

    try:
        all_forms = odk.refresh_forms()
    except Exception:
        logging.error(traceback.format_exc())

    return HttpResponse(json.dumps({'error': False, 'all_forms': all_forms}))


def get_or_create_csrf_token(request):
    token = request.META.get('CSRF_COOKIE', None)
    if token is None:
        token = csrf._get_new_csrf_string()
        request.META['CSRF_COOKIE'] = token
    request.META['CSRF_COOKIE_USED'] = True
    return token


def serve_static_files(request, path, insecure=False, **kwargs):
    """
    Serve static files below a given point in the directory structure or
    from locations inferred from the staticfiles finders.
    To use, put a URL pattern such as::
        from django.contrib.staticfiles import views
        url(r'^(?P<path>.*)$', views.serve)
    in your URLconf.
    It uses the django.views.static.serve() view to serve the found files.
    """

    if not settings.DEBUG and not insecure:
        raise Http404
    normalized_path = posixpath.normpath(unquote(path)).lstrip('/')
    absolute_path = finders.find(normalized_path)
    if not absolute_path:
        if path.endswith('/') or path == '':
            raise Http404("Directory indexes are not allowed here.")
        raise Http404("'%s' could not be found" % path)
    document_root, path = os.path.split(absolute_path)
    return static.serve(request, path, document_root=document_root, **kwargs)


def manage_mappings(request):
    csrf_token = get_or_create_csrf_token(request)

    odk = OdkForms()
    all_forms = odk.get_all_forms()
    (db_tables, tables_columns) = odk.get_db_tables()
    mappings = odk.mapping_info()

    page_settings = {
        'page_title': "%s | Home" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Manage ODK and Database Mappings',
        'db_tables': json.dumps(db_tables),
        'tables_columns': json.dumps(tables_columns),
        'all_forms': json.dumps(all_forms),
        'mappings': json.dumps(mappings)
    }
    return render(request, 'manage_mappings.html', page_settings)


def edit_mapping(request):
    odk = OdkForms()
    if (request.get_full_path() == '/edit_mapping/'):
        response = odk.edit_mapping(request)

    return HttpResponse(json.dumps(response))


def create_mapping(request):
    odk = OdkForms()
    mappings = odk.save_mapping(request)
    return return_json(mappings)


def delete_mapping(request):
    odk = OdkForms()
    mappings = odk.delete_mapping(request)
    return return_json(mappings)


def clear_mappings(request):
    odk = OdkForms()
    mappings = odk.clear_mappings()
    return return_json(mappings)


def return_json(mappings):
    to_return = json.dumps(mappings)
    response = HttpResponse(to_return, content_type='text/json')
    response['Content-Message'] = to_return
    return response

def return_polygons(mappings):
    to_return = json.dumps(mappings)
    response = HttpResponse(to_return)
    response['Content-Type'] = 'application/json'
    response['Content-Message'] = to_return
    return response


def validate_mappings(request):
    odk = OdkForms()
    (is_fully_mapped, is_mapping_valid, comments) = odk.validate_mappings()

    to_return = {'error': False, 'is_fully_mapped': is_fully_mapped, 'is_mapping_valid': is_mapping_valid, 'comments': comments}
    return return_json(to_return)


def manual_data_process(request):
    odk = OdkForms()
    is_dry_run = json.loads(request.POST['is_dry_run'])
    (is_success, comments) = odk.manual_process_data(is_dry_run)

    to_return = {'error': is_success, 'comments': comments}
    return return_json(to_return)


def delete_processed_data(request):
    odk = OdkForms()
    (is_success, comments) = odk.delete_processed_data()

    to_return = {'error': is_success, 'comments': comments}
    return return_json(to_return)


def processing_errors(request):
    csrf_token = get_or_create_csrf_token(request)
    page_settings = {
        'page_title': "%s | Processing Errors" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'section_title': 'Processing Errors'
    }
    return render(request, 'processing_errors.html', page_settings)


def fetch_processing_errors(request):
    cur_page = json.loads(request.GET['page'])
    per_page = json.loads(request.GET['perPage'])
    offset = json.loads(request.GET['offset'])
    sorts = json.loads(request.GET['sorts']) if 'sorts' in request.GET else None
    queries = json.loads(request.GET['queries']) if 'queries' in request.GET else None

    odk = OdkForms()
    (is_success, proc_errors) = odk.processing_errors(cur_page, per_page, offset, sorts, queries)
    to_return = json.dumps(proc_errors)

    response = HttpResponse(to_return, content_type='text/json')
    response['Content-Message'] = to_return
    return response


def fetch_single_error(request):
    err_id = json.loads(request.POST['err_id'])
    odk = OdkForms()
    (is_success, cur_error, r_sub) = odk.fetch_single_error(err_id)

    to_return = {'error': is_success, 'err_json': cur_error, 'raw_submission': r_sub}
    return return_json(to_return)


def map_visualization(request):
    csrf_token = get_or_create_csrf_token(request)
    odk = OdkForms()
    map_settings = odk.fetch_base_map_settings()
    page_settings = {
        'page_title': "%s | Map Based Visualizations" % settings.SITE_NAME,
        'csrf_token': csrf_token,
        'map_title': 'Map Based Visualization',
        'section_title': 'Map Based Visualization',
        'map_settings': json.dumps(map_settings)
    }
    return render(request, 'map_visualizations.html', page_settings)


def first_level_geojson(request):
    odk = OdkForms()
    c_code = json.loads(request.GET['c_code'])
    cur_polygons = odk.first_level_geojson(int(c_code))

    return HttpResponse(json.dumps(cur_polygons), content_type='application/json')


def save_json_edits(request):
    err_id = json.loads(request.POST['err_id'])
    odk = OdkForms()
    (is_error, cur_error) = odk.save_json_edits(err_id, json.loads(request.POST['json_data']))

    to_return = {'error': is_error, 'message': cur_error}
    return return_json(to_return)


def process_single_submission(request):
    err_id = json.loads(request.POST['err_id'])
    odk = OdkForms()
    (is_error, cur_error) = odk.process_single_submission(err_id)

    to_return = {'error': is_error, 'message': cur_error}
    return return_json(to_return)


def zip_response(json_data):
    gzip_buffer = IO()
    gzip_file = gzip.GzipFile(mode='wb', fileobj=gzip_buffer)
    gzip_file.write(json_data)
    gzip_file.close()

    response = HttpResponse(content_type='application/json')

    response.data = gzip_buffer.getvalue()
    response.headers['Content-Encoding'] = 'gzip'
    response.headers['Vary'] = 'Accept-Encoding'
    response.headers['Content-Length'] = len(response.data)

    return response