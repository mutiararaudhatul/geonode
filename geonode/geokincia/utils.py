from django.conf import settings

from geonode.layers.models import Dataset, UserCollectorStorage
from geonode.geoserver.createlayer.utils import create_dataset
from . import db_utils

import requests
import importlib
import subprocess
import re
import os
import logging

logger = logging.getLogger(__name__)

def get_class(full_class_name):
    module_name = full_class_name[:full_class_name.rfind('.')]
    class_name = full_class_name[full_class_name.rfind('.')+1 :]
    module = importlib.import_module(module_name)
    init_class =  getattr(module, class_name)
    return init_class()

def process_csv(csv_file, user, layer_id):
    logger.debug(f'process_csv')
    try:
        layer = Dataset.objects.get(id=layer_id)
    except Dataset.DoesNotExist or UserCollectorStorage.DoesNotExist:
        logger.warning(f"Layers {layer_id} or collector does not exist!")
        return

    src_layer = layer.name
    logger.debug(f'process_csv not use aggregate data')
    user_collector = UserCollectorStorage.objects.get(dataset=layer, user__username=user)
    if not user_collector.intermediate_dataset_name:
            #if not sync create dataset
        new_dataset = create_new_collector_dataset(layer, user_collector.user.username)
        target_layer = new_dataset.name
        user_collector.intermediate_dataset_name = target_layer
        user_collector.save()
    else:
        target_layer = user_collector.intermediate_dataset_name
    logger.debug(f'process_csv use aggregate data')
    db_utils.load_from_csv(settings.DATABASES['GEOSERVER'], csv_file, target_layer, layer.use_aggregate_data, src_layer)

def process_shp(shp_file, user, layer_id):
    logger.debug(f'process_shp')
    s = subprocess.run(f'ogr2ogr -f CSV -overwrite out.csv {os.path.basename(shp_file)} -lco GEOMETRY=AS_WKT', capture_output=True, cwd=os.path.dirname(shp_file), shell=True)
    if s.returncode != 0:
        logger.debug(f'fail conver shp to csv {s.stderr} {s.stdout}')
        raise Exception
    process_csv(os.path.join(os.path.dirname(shp_file), 'out.csv'), user, layer_id)


def create_new_collector_dataset(layer, username):
    ATTRIBUTE_TYPE_MAPPING = {'xsd:string': 'string', 'xsd:int': 'integer', 'xsd:float': 'float', 'xsd:dateTime': 'date'}
    ATTRIBUTE_GEO_PREFIX = 'gml:'
    ATTRIBUTE_SKIP_PREFIX = 'internal_'
    ATTRIBUTE_ID = ('fid', 'gid')
    ATTRIBUTE_GEO_TYPES = r'(MultiPolygon|Polygon|MultiLineString|LineString|MultiPoint|Point)'
    geometry_type = ''
    attributes = {}
    for attribute in  layer.attribute_set.all():
        if attribute.attribute_type.startswith(ATTRIBUTE_GEO_PREFIX):
            geometry_type = re.findall(ATTRIBUTE_GEO_TYPES, attribute.attributeType, re.IGNORECASE)
            if len(geometry_type) < 1:
                raise Exception
            continue
            
        if attribute.attribute.startswith(ATTRIBUTE_SKIP_PREFIX) or attribute.attribute in ATTRIBUTE_ID:
            continue
        attributes[attribute.attribute] = ATTRIBUTE_TYPE_MAPPING[attribute.attribute_type]

#create_dataset(name, title, owner_name, geometry_type, attributes=None)
    gid = layer.group.id if layer.group else None
    return create_dataset(f'{layer.name}_{username}', layer.title, layer.owner.username, geometry_type[0], attributes, True, gid)

def download_source_dataset(ws, name):
    url = '%swfs?service=wfs&version=1.0.0&request=GetFeature&typeName=%s:%s&outputformat=SHAPE-ZIP' % (settings.GEOSERVER_LOCATION, ws, name)
    logger.debug(f'get source {url}')
    local_filename = os.path.join(settings.GEOKINCIA['WORKING_DIR'], 'source', name + '.zip')
    if not os.path.exists(os.path.dirname(local_filename)):
        os.makedirs(os.path.dirname(local_filename))
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    logger.debug(f'source save to {local_filename}')
    return local_filename
