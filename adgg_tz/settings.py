"""
Django settings for marsabit project.

Generated by 'django-admin startproject' using Django 1.10.5.

For more information on this file, see
https://docs.djangoproject.com/en/1.10/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.10/ref/settings/
"""

import os
import json

SITE_ROOT = os.path.dirname(os.path.realpath(__file__))
if 'MYSQL_DATABASE' in os.environ:
    STATICFILES_DIRS = (
        os.path.join('/opt/azizi_amp/static/'),
    )
else:
    STATICFILES_DIRS = (
        os.path.join(SITE_ROOT, '/www/adgg_v2/static/'),
    )

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.10/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'xw9xyc$nyym$q0-3-pozdek-f0o_z1xpktpm8ex36k9g&0464v'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ["*"]


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_crontab',
    'livereload',
    'raven.contrib.django.raven_compat',

    'adgg_tz'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'livereload.middleware.LiveReloadScript',
]

ROOT_URLCONF = 'adgg_tz.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.jinja2.Jinja2',
        'DIRS': [os.path.join(BASE_DIR, 'templates/jinja2')],
        'APP_DIRS': True,
        'OPTIONS': {'environment': 'adgg_tz.jinja2_settings.environment',},
    },
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates/django')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'adgg_tz.wsgi.application'

SITE_NAME = 'ADGG v2'

DEFAULT_REPORTING_PERIOD = 30

SESSION_ENGINE = 'django.contrib.sessions.backends.signed_cookies'

# Database
# https://docs.djangoproject.com/en/1.10/ref/settings/#databases

# either use the environment variables or variables defined in a config file
if 'MYSQL_DATABASE' in os.environ:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': os.environ['MYSQL_DATABASE'],
            'USER': os.environ['MYSQL_USER'],
            'PASSWORD': os.environ['MYSQL_PASSWORD'],
            'HOST': os.environ['MYSQL_HOST'],
            'PORT': os.environ['MYSQL_PORT']
        },
        'mapped': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': os.environ['MAPPED_DATABASE'],
            'USER': os.environ['MAPPED_USER'],
            'PASSWORD': os.environ['MAPPED_PASSWORD'],
            'HOST': os.environ['MAPPED_HOST'],
            'PORT': os.environ['MAPPED_PORT']
        }
    }

    ONADATA_SETTINGS = {
        'HOST': os.environ['ONA_HOST'],
        'USER': os.environ['ONA_USER'],
        'PASSWORD': os.environ['ONA_PASSWORD'],
        'API_TOKEN': os.environ['ONA_API_TOKEN']
    }
else:
    with open('adgg_tz/app_config.json') as config_file:
        configs = json.load(config_file)
        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.mysql',
                'NAME': configs['default']['db'],
                'USER': configs['default']['user'],
                'PASSWORD': configs['default']['passwd'],
                'HOST': configs['default']['host'],
                'PORT': configs['default']['port']
            },
            'mapped': {
                'ENGINE': 'django.db.backends.mysql',
                'NAME': configs['mapped']['db'],
                'USER': configs['mapped']['user'],
                'PASSWORD': configs['mapped']['passwd'],
                'HOST': configs['mapped']['host'],
                'PORT': configs['mapped']['port']
            }
        }

        ONADATA_SETTINGS = {
            'HOST': configs['onadata']['host'],
            'USER': configs['onadata']['user'],
            'PASSWORD': configs['onadata']['passwd'],
            'API_TOKEN': configs['onadata']['api_token']
        }

CRONJOBS = [
    # ('*/5 * * * *', 'marsabit.odk_forms.auto_process_submissions', '>> /tmp/marsabit_cron.log')
]


# Password validation
# https://docs.djangoproject.com/en/1.10/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.10/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Africa/Nairobi'

USE_I18N = True

USE_L10N = True

USE_TZ = False


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.10/howto/static-files/

STATIC_URL = 'static/'

DEFAULT_LOCALE = 'English'

LOCALES = {
    'English': 'en'
}

LOOKUP_TABLE = 'dictionary_items'

# The number of records to use for the dry run
DRY_RUN_RECORDS = 30

ERR_CODES = {
    'duplicate': {'CODE': 10001, 'TAG': 'DUPLICATE'},
    'data_error': {'CODE': 10002, 'TAG': 'INVALID DATA'},
    'fk_error': {'CODE': 10003, 'TAG': 'MISSING FK'},
    'unknown': {'CODE': 10004, 'TAG': 'UNKNOWN ERROR'}
}

JOINER = ' - '

GPS_REGEX = '-?\d{1,3}\.\d+\s-?\d{1,3}\.\d+\s\d{1,5}\.\d+\s\d{1,4}\.\d{1,2}'
