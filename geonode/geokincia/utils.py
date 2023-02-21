from django.conf import settings

from geonode.layers.models import Dataset, UserCollectorStorage
from geonode.geoserver.createlayer.utils import create_dataset
from . import db_utils
from requests.auth import HTTPBasicAuth
from datetime import date, datetime

import requests
import importlib
import subprocess
import re
import os
import logging
import json
import csv

logger = logging.getLogger(__name__)

ATTACHMENT = ('attachment', 'Attachment', 'attachments', 'Attachments')

class AttachmentNotFound(Exception):
    pass

def get_class(full_class_name):
    module_name = full_class_name[:full_class_name.rfind('.')]
    class_name = full_class_name[full_class_name.rfind('.')+1 :]
    module = importlib.import_module(module_name)
    init_class =  getattr(module, class_name)
    return init_class()

def all_attachment_exists(csv_file):
    dirname = os.path.dirname(csv_file)
    with open(csv_file, 'r') as f:
        processed_csv = csv.reader(f, dialect='excel')
        header = next(processed_csv)
        name_att = ''
        for i in range(len(header)):
            if header[i] in ATTACHMENT:
                name_att = header[i]
                break
        if not name_att:
            return
        index_att = header.index(name_att)
        for r in processed_csv:
            for att_f in r[index_att].split(','):
                att_f = att_f.strip()
                if att_f and not os.path.exists(os.path.join(dirname, att_f)):
                    logger.info(f'att check {os.path.join(dirname, att_f)} not found')
                    raise AttachmentNotFound
    return True

def add_time_check(csv_file):
    with open(os.path.join(os.path.dirname(csv_file), '.gn-timecheck'), 'w') as f:
        f.write(str(int(datetime.now().timestamp())))

def process_csv(csv_file, user, layer_id):
    logger.debug(f'process_csv')
    try:
        layer = Dataset.objects.get(id=layer_id)
    except Dataset.DoesNotExist or UserCollectorStorage.DoesNotExist:
        logger.warning(f"Layers {layer_id} or collector does not exist!")
        return

    src_layer = layer.name
    user_collector = UserCollectorStorage.objects.get(dataset=layer, user__username=user)
    if not user_collector.intermediate_dataset_name:
            #if not sync create dataset
        new_dataset = create_new_collector_dataset(layer, user_collector.user.username)
        target_layer = new_dataset.name
        user_collector.intermediate_dataset_name = target_layer
        user_collector.save()
    else:
        target_layer = user_collector.intermediate_dataset_name
    db_utils.load_from_csv('datastore', csv_file, target_layer, layer.use_aggregate_data, src_layer, user=user_collector.user.username)
    truncate_geoserver_cache(layer.workspace, target_layer)
    if layer.use_aggregate_data:
        truncate_geoserver_cache(layer.workspace, layer.name)

def process_shp(shp_file):
    logger.debug(f'process_shp')
    if os.path.exists(os.path.join(os.path.dirname(shp_file), 'out.csv')):
        os.remove(os.path.join(os.path.dirname(shp_file), 'out.csv'))
    s = subprocess.run(f'ogr2ogr -f CSV -overwrite out.csv {os.path.basename(shp_file)} -lco GEOMETRY=AS_WKT -t_srs "EPSG:4326"', capture_output=True, cwd=os.path.dirname(shp_file), shell=True)
    if s.returncode != 0:
        logger.debug(f'fail conver shp to csv {s.stderr} {s.stdout}')
        raise Exception


def create_new_collector_dataset(layer, username):
    ATTRIBUTE_TYPE_MAPPING = {'xsd:string': 'string', 'xsd:int': 'integer', 'xsd:integer': 'integer','xsd:float': 'float', 'xsd:dateTime': 'date', 
    'xsd:double': 'float', 'xsd:date': 'date', 'xsd:decimal': 'float'}
    ATTRIBUTE_GEO_PREFIX = 'gml:'
    pri_key = db_utils.get_primary_key('datastore', layer.name)
    ATTRIBUTE_SKIP = ('___update', '___id', '___att', 'created_by', 'created_at', 'updated_by', 'updated_at', 'lastupdate', pri_key)
    ATTRIBUTE_GEO_TYPES = r'(MultiPolygon|Polygon|MultiLineString|LineString|MultiPoint|Point)'
    geometry_type = ''
    attributes = {}
    for attribute in layer.attribute_set.all():
        if attribute.attribute_type.startswith(ATTRIBUTE_GEO_PREFIX):
            geometry_type = re.findall(ATTRIBUTE_GEO_TYPES, attribute.attribute_type, re.IGNORECASE)
            if len(geometry_type) < 1:
                raise Exception
            continue
            
        if attribute.attribute in ATTRIBUTE_SKIP:
            continue
        attributes[attribute.attribute] = ATTRIBUTE_TYPE_MAPPING[attribute.attribute_type]

#create_dataset(name, title, owner_name, geometry_type, attributes=None)
    gid = layer.group.id if layer.group else None
    return create_dataset(f'{layer.name}_{username}', f'{layer.title} - {username}', layer.owner.username, geometry_type[0], json.dumps(attributes), True, gid, False)

def download_source_dataset(ws, name, cwd):
    url = '%swfs?service=wfs&version=1.0.0&request=GetFeature&typeName=%s:%s&outputformat=SHAPE-ZIP' % (settings.GEOSERVER_LOCATION, ws, name)
    logger.debug(f'get source {url}')
    local_filename = os.path.join(cwd, 'source', name + '.zip')
    basic = HTTPBasicAuth(settings.OGC_SERVER['default']['USER'], settings.OGC_SERVER['default']['PASSWORD'])
    if not os.path.exists(os.path.dirname(local_filename)):
        os.makedirs(os.path.dirname(local_filename))
    
    with requests.get(url, stream=True, auth=basic) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk:
                    f.write(chunk)
    logger.debug(f'source save to {local_filename}')
    return local_filename

def truncate_geoserver_cache(ws, name):
    data = f'<truncateLayer><layerName>{ws}:{name}</layerName></truncateLayer>'
    url = '%sgwc/rest/masstruncate'  % (settings.GEOSERVER_LOCATION,)
    basic = HTTPBasicAuth(settings.OGC_SERVER['default']['USER'], settings.OGC_SERVER['default']['PASSWORD'])
    r = requests.post(url, auth=basic, data=data)
    