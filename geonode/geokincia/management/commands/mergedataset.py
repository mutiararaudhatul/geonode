from django.core.management.base import BaseCommand
from django.conf import settings
from geonode.geokincia import db_utils, utils
from geonode.layers.models import Dataset, UserCollectorStorage
import os
import traceback
import argparse

class Command(BaseCommand):
    help = (
            "Gabung dataset"
        )

    def add_arguments(self, parser):
        parser.add_argument("-s", "--source", dest="source", help="Source dataset")
        parser.add_argument("-u", "--users", dest="users", help="users")
        parser.add_argument("-c", "--child", dest="child", help="child dataset")
        parser.add_argument("-i", "--id", dest="fid", help="field id")
        parser.add_argument("-a", "--att_only", dest="att_only", help="attribut only", action=argparse.BooleanOptionalAction)


    
    def handle(self, *args, **options):
        source = options.get("source")
        users = options.get("users")
        child = options.get("child")
        if not source:
            self.print_help('manage.py', 'mergedataset')

        if users:
            uc_datasets = UserCollectorStorage.objects.filter(dataset__name=source, user__username__in=users.split(','))
            for uc_dataset in uc_datasets:
                try:
                    db_utils.copy_table('datastore', uc_dataset.intermediate_dataset_name, uc_dataset.dataset.name)
                    db_utils.empty_table('datastore', uc_dataset.intermediate_dataset_name)
                except:
                    print(f'fail to merge {uc_dataset.intermediate_dataset_name}')
            utils.truncate_geoserver_cache(uc_dataset.dataset.workspace, uc_dataset.dataset.name)
        elif child:
            fid = options.get("fid")
            att_only = options.get("att_only")
            layer = Dataset.objects.get(name=source)
            print(f'source dataset: {layer.name}')
            layer_child = Dataset.objects.get(name=child)
            print(f'child dataset: {layer_child.name}')
            if fid:
                db_utils.execute_query('datastore', f'''update {child} set "___id"="{fid}", "{fid}" = '' ''', None, False)
            db_utils.copy_table('datastore', layer_child.name, layer.name, not att_only)
            utils.truncate_geoserver_cache(layer.workspace, layer.name)
            
        
        