from geonode.geokincia import db_utils

from django.shortcuts import render
from django.contrib.auth import get_user_model
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods

from geonode.layers.models import Dataset, UserCollectorStorage
from .tasks import process_uploaded_data_task
import base64
import json
import logging

logger = logging.getLogger(__name__)

def webhook(request, storage_provider):
    process_uploaded_data_task.delay(storage_provider)
    return HttpResponse(status=200)

@require_http_methods(["GET"])
def get_attachments(request, table, nid, id):
    if not table or not nid or not id:
        return HttpResponse(status=404)
    try:
    
        ret = db_utils.execute_query('datastore', f'select "___att" from \"{table}\" where \"{nid}\"={id}', None, True, False)
        if len(ret) < 1:
            return HttpResponse(status=404)
        if len(ret[0]) < 1:
            return HttpResponse(status=404)
        _attachments = ret[0][0].split(';')
        attachments = []
        for _att in _attachments:
            if _att:
                attr = _att.split('#')
                if attr and len(attr) > 2:
                    attachments.append({'url': f'/static/attachment/{attr[0]}', 'type': attr[1], 'tanggal': attr[2]})
        return HttpResponse(json.dumps(attachments), content_type='application/json')
    except:
            return HttpResponse(status=404)



