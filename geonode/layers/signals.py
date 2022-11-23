from asyncio.log import logger
from unicodedata import name
from django.dispatch import receiver
from django.conf import settings
from django.db.models.signals import post_delete, post_save

from .models import Dataset, UserCollectorStorage
from geonode.base.models import Configuration
from geonode.geokincia.tasks import delete_file_task, process_uploaded_data_task

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

# @receiver(post_save, sender=Configuration)
# def collect_collector(sender, instance, *args, **kwargs):
#     if not (instance.read_only or instance.maintenance):
#         for storage in settings.GEOKINCIA['STORAGE'].keys():
#             process_uploaded_data_task.delay(storage)

