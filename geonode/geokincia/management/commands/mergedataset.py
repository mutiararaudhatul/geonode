from django.core.management.base import BaseCommand
from django.conf import settings
from geonode.geokincia import db_utils, utils
from geonode.layers.models import Dataset, UserCollectorStorage
import os
import traceback

class Command(BaseCommand):
    help = (
            "Hapus file attachment yang tidak terpakai"
        )

    def add_arguments(self, parser):
        parser.add_argument("-s", "--source", dest="source", help="Source dataset")
        parser.add_argument("-u", "--users", dest="users", help="users")
        parser.add_argument("-d", "--delete", dest="delete", help="Delete merged dataset")

    
    def handle(self, *args, **options):
        source = options.get("source")
        users = options.get("users")
        if not source or not users:
            self.print_help('manage.py', 'mergedataset')
        uc_datasets = UserCollectorStorage.objects.filter(dataset__name=source, user__username__in=users.split(','))
        for uc_dataset in uc_datasets:
            try:
                db_utils.copy_table('datastore', uc_dataset.intermediate_dataset_name, uc_dataset.dataset.name)
                db_utils.empty_table('datastore', uc_dataset.intermediate_dataset_name)
            except:
                print(f'fail to merge {uc_dataset.intermediate_dataset_name}')
        utils.truncate_geoserver_cache(uc_dataset.dataset.workspace, uc_dataset.dataset.name)
        