from django.core.management.base import BaseCommand
from django.conf import settings
from geonode.geokincia import db_utils, utils
from geonode.layers.models import Dataset, UserCollectorStorage
import os
import traceback

class Command(BaseCommand):
    help = (
            "Update url attachment"
        )

    def add_arguments(self, parser):
        parser.add_argument("-t", "--table", dest="tables", help="table")
        parser.add_argument("-f", "--force", dest="force", help="force update")

    
    def handle(self, *args, **options):
        tables = options.get("tables")
        force = options.get("force")
        if not tables:
            self.print_help('manage.py', 'updateurlatt')
        for table in tables.split(','):
            try:
                print(f'processing table {table}')
                if force and force.lower() == 'y':
                    db_utils.update_url_att('datastore', None, table, True)
                else:
                    db_utils.update_url_att('datastore', None, table)
            except:
                print(f'fail to update {table}')
        