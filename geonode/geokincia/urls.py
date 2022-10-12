from django.conf.urls import url
from .views import webhook

urlpatterns = [ url(r'^webhook/(?P<storage_provider>\w+)/$', webhook , name="webhook"),]