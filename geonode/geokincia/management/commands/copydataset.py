from django.core.management.base import BaseCommand
from django.conf import settings
from geonode.geokincia import db_utils, utils
from geonode.layers.models import Dataset
import os
import traceback

class Command(BaseCommand):
    help = (
            "Copy dataset"
        )

    def add_arguments(self, parser):
        parser.add_argument("-s", "--source", dest="source", help="Source dataset name")
        parser.add_argument("-t", "--target", dest="target", help="target dataset")

    
    def handle(self, *args, **options):
        source = options.get("source")
        target = options.get("target")
        if not source or not target:
            self.print_help('manage.py', 'copydataset')

        try:
            layer = Dataset.objects.get(name=source)
            new_ds = utils.create_new_collector_dataset(layer, None, is_collector_dataset=False, new_name=target)
            print(f'new dataset: {new_ds.name}')
        except:
            print(f'fail to copy dataset {source}')