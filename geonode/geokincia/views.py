from django.shortcuts import render
from django.contrib.auth import get_user_model
from django.http import HttpResponse

from geonode.layers.models import Dataset, UserCollectorStorage
from .tasks import process_uploaded_data_task

def webhook(request, storage_provider):
    process_uploaded_data_task.delay(storage_provider)
    return HttpResponse(status=200)
