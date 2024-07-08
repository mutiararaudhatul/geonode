from . import views
from django.contrib import admin
from django.conf.urls import url
from django.views.generic import TemplateView

app_name = 'report'

urlpatterns = [
    url(r'^$', TemplateView.as_view(template_name='report/home.html'), name='home'),
    url(r'^pelaporan/$', views.pelaporan, name='pelaporan'),
    url(r'^handle_form_lapor/$', views.handle_form_lapor, name='handle_form_lapor'),
    url(r'^admin/login/$', admin.site.login, name='login'),
]