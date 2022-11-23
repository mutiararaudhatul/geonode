#########################################################################
#
# Copyright (C) 2017 OSGeo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################

"""celery tasks for geonode.layers."""
from geonode.celery_app import app
from celery.utils.log import get_task_logger

from geonode.layers.models import Dataset, UserCollectorStorage
from geonode.resource.manager import resource_manager

logger = get_task_logger(__name__)


@app.task(
    bind=True,
    name='geonode.layers.tasks.delete_dataset',
    queue='cleanup',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False)
def delete_dataset(self, dataset):
    if type(dataset) is list:
        for dataset_name in dataset:
            delete_dataset_by_id_or_name(dataset_name)
    else:
        delete_dataset_by_id_or_name(dataset)        



def delete_dataset_by_id_or_name(dataset_id_or_name):
    """
    Deletes a layer.
    """
    try:
        if type(dataset_id_or_name) is str:
            layer = Dataset.objects.get(name=dataset_id_or_name)
        else:
            layer = Dataset.objects.get(id=dataset_id_or_name)
    except Dataset.DoesNotExist:
        logger.warning(f"Layers {dataset_id_or_name} does not exist!")
        return
    logger.debug(f'Deleting Dataset {layer}')
    if layer.is_data_collector:
        ds_names = [ uc.intermediate_dataset_name for uc in UserCollectorStorage.objects.filter(dataset=layer) ]
        collector_ds = Dataset.objects.filter(name__in=ds_names)
        for ds in collector_ds:
            resource_manager.delete(ds.uuid)        
        logger.debug(f'Deleting Collector dataset Dataset {layer}')
    resource_manager.delete(layer.uuid)
