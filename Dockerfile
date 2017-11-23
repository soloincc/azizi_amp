FROM python:2.7

MAINTAINER Wangoru Kihara wangoru.kihara@badili.co.ke

# Install build deps, then run `pip install`, then remove unneeded build deps all in a single step. Correct the path to your production requirements file, if needed.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
    mysql-client \
    libmysqlclient-dev
#     git \
#     python \
#     python-dev \
#     python-setuptools \
#     python-pip \
#     nginx \

# install uwsgi now because it takes a little while
RUN pip install --upgrade pip && \
    pip install uwsgi

# Copy your application code to the container (make sure you create a .dockerignore file if any large files or directories should be excluded)
RUN mkdir /opt/azizi_amp/

# Copy the requirements file and install the requirements
COPY requirements.txt /opt/azizi_amp/
RUN pip install -r /opt/azizi_amp/requirements.txt

# add (the rest of) our code
COPY . /opt/azizi_amp/

# uWSGI will listen on this port
# EXPOSE 8089

# CMD ["uwsgi", "--ini", "/opt/azizi-amp/default_uwsgi.ini"]

WORKDIR /opt/azizi_amp

# Manually start the server for now
# CMD python manage.py runserver

# Add any custom, static environment variables needed by Django or your settings file here:
# ENV DJANGO_SETTINGS_MODULE=azizi_amp.settings

# uWSGI configuration (customize as needed):
# ENV UWSGI_VIRTUALENV=/venv UWSGI_WSGI_FILE=azizi_amp/uwsgi.ini UWSGI_HTTP=:8089 UWSGI_MASTER=1 UWSGI_WORKERS=2 UWSGI_THREADS=8 UWSGI_UID=1000 UWSGI_GID=2000 UWSGI_LAZY_APPS=1 UWSGI_WSGI_ENV_BEHAVIOR=holy

# Call collectstatic (customize the following line with the minimal environment variables needed for manage.py to run):
# RUN DATABASE_URL=none /venv/bin/python manage.py collectstatic --noinput

# Start uWSGI
# CMD ["/venv/bin/uwsgi", "--http-auto-chunked", "--http-keepalive"]

ADD scripts /opt/scripts
WORKDIR /opt/scripts
RUN chmod a+x *.sh

ENTRYPOINT ["/opt/scripts/entrypoint.sh"]