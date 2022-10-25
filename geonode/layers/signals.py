from asyncio.log import logger
from unicodedata import name
from django.dispatch import receiver
from django.db.models.signals import post_delete, post_save
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import Permission, Group

from .models import Dataset, UserCollectorStorage
from geonode.base.models import ResourceBase
from geonode.geokincia.tasks import delete_file_task
from guardian.models import GroupObjectPermission

import logging

logger = logging.getLogger(__name__)

@receiver(post_delete, sender=UserCollectorStorage)
def delete_intermediate_storage(sender, instance, *args, **kwargs):
    logger.debug(f'delete folder {instance.folder}')
    if instance.folder:
        delete_file_task.delay(instance.dataset.intermediate_storage, instance.folder)
    
@receiver(post_delete, sender=Dataset)
def delete_dataset(sender, instance, *args, **kwargs):
    logger.debug(f'delete dataset {instance.id}')
    if instance.is_data_collector and instance.source_url:
        delete_file_task.delay(instance.intermediate_storage, instance.file_path)