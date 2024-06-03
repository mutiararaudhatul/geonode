from django.conf.urls import url
from .views import webhook, get_attachments

urlpatterns = [ url(r'^webhook/(?P<storage_provider>\w+)/$', webhook , name="webhook"),
               url(r'^att/detail/(?P<table>[A-Za-z0-9=-_]+)/(?P<nid>[A-Za-z0-9=-_]+)/(?P<id>[0-9]+)/$', get_attachments , name="att_detail"),]