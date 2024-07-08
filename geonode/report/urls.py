# from django.urls import path
from . import views
from django.contrib import admin
from django.conf.urls import url

urlpatterns = [
    # url(r'^$', views.home, name='home'),
    url(r'^$', TemplateView.as_view(template_name='report/index.html'), name='report_browse'),
    # url(r'^home/$', views.home, name='home'),
    url(r'^pelaporan/$', views.pelaporan, name='pelaporan'),
    url(r'^handle_form_lapor/$', views.handle_form_lapor, name='handle_form_lapor'),
    url(r'^laporan_list/$', views.laporan_list, name='laporan_list'), 
    url(r'^laporan/<int:id>/$', views.laporan_detail, name='laporan_detail'),

    url(r'^admin/login/$', admin.site.login, name='login'),
]
