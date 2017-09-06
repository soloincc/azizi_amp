"""marsabit URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls import include

from adgg_tz import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^accounts/', include('django.contrib.auth.urls')),
    url(r'static/(?P<path>.*)$', views.serve_static_files),
    url(r'^$', views.show_dashboard, name='landing_page'),
    url(r'^sdss$', views.show_dashboard, name='landing_page'),
    url(r'^home$', views.show_dashboard, name='landing_page'),
    url(r'^dashboard$', views.show_dashboard, name='dashboard'),
    url(r'^login$', views.login_page, name='login_page'),
    url(r'^logout$', views.logout_view, name='logout'),
    url(r'^form_structure/', views.form_structure, name='form_structure'),
    url(r'^update_db/', views.update_db, name='update_db'),
    url(r'^form_structure/', views.form_structure, name='form_structure'),
    url(r'^download/$', views.download_page, name='download_page'),
    url(r'^manage_views/$', views.manage_views, name='manage_views'),
    url(r'^manage_mappings/$', views.manage_mappings, name='manage_mappings'),
    url(r'^edit_view/$', views.modify_view, name='modify_view'),
    url(r'^delete_view/$', views.modify_view, name='modify_view'),
    url(r'^get_data/$', views.download_data, name='download_data'),
    url(r'^refresh_forms/$', views.refresh_forms, name='refresh_forms'),
    url(r'^create_mapping/$', views.create_mapping, name='create_mapping'),
    url(r'^edit_mapping/$', views.edit_mapping, name='edit_mapping'),
    url(r'^delete_mapping/$', views.delete_mapping, name='delete_mapping'),
    url(r'^validate_mappings/$', views.validate_mappings, name='validate_mappings'),
    url(r'^clear_mappings/$', views.clear_mappings, name='clear_mappings'),
    url(r'^manual_data_process/$', views.manual_data_process, name='manual_data_process'),
]
