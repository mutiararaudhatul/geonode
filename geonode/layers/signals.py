from django.dispatch import receiver
from django.db.models.signals import pre_delete, post_save

from .models import Dataset, UserCollectorStorage
from geokincia.tasks import delete_file_task, prepare_dataset_task

@receiver(pre_delete, sender=UserCollectorStorage)
def delete_intermediate_storage(sender, instance, *args, **kwargs):
    delete_file_task.delay(instance.dataset.intermediate_storage, f'upload/{instance.folder}')
    
@receiver(post_save,sender=Dataset)
def process_intermediate_storage(sender, instance, created, **kwargs):
    if not created and not instance.is_collector_dataset:
        if not instance.is_data_collector and instance.source_url:
            delete_file_task.delay(instance.intermediate_storage, f'source/{instance.file_path}')
        elif instance.is_data_collector and not instance.source_url:
            prepare_dataset_task.delay(instance.pk)