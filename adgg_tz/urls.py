
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls import include

from adgg_tz import views

urlpatterns = [
    url(r'^', include('odk_dashboard.urls')),
    url(r'^admin/', admin.site.urls),
    url(r'^$', views.show_landing, name='landing_page'),
    url(r'^home$', views.show_dashboard, name='landing_page'),
    url(r'^dashboard$', views.show_dashboard, name='dashboard'),
    url(r'^accounts/', include('django.contrib.auth.urls')),
    url(r'static/(?P<path>.*)$', views.serve_static_files),
]
