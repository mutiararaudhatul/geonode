from selectors import EpollSelector
from django.core.management.base import BaseCommand
from django.conf import settings
from geonode.geokincia import db_utils
from geonode.layers.models import Dataset
import os
import traceback

class Command(BaseCommand):
    help = (
            "Hapus file attachment yang tidak terpakai"
        )

    def _process_table(self, all_att_files, name):
        q = 'select "___att" from "%s"' % name
        for att in db_utils.execute_query('datastore', q, None, True,False):
            _att = att[0].split(';') if att[0] else [] 
            for att_pair in _att:
                if len(att_pair) > 0:
                    att_pair_name = att_pair.split('#')
                    if len(att_pair_name) > 0:
                        try:
                            all_att_files.remove(att_pair_name[0])
                        except KeyError:
                            pass

    def _process(self, check=False):
        att_dir = settings.GEOKINCIA['ATTACHMENT_DIR']
        datasets = Dataset.objects.all()
        all_att_files = set(filter(lambda f: not f.startswith('geokincia.'), os.listdir(att_dir)))
        for dataset in datasets:
            print(f'processing {dataset.name}')
            if dataset.attribute_set.filter(attribute='___att').count() > 0:
                self._process_table(all_att_files, dataset.name)
            else:
                print(f'dataset {dataset.name} has no attachment. Skipping.')
        
        for name in all_att_files:
            if check:
                print(f'{os.path.join(att_dir, name)}')
            else:
                try:
                    print(f'deleting {os.path.join(att_dir, name)}')
                    os.remove(os.path.join(att_dir, name))
                except:
                    print(f'\tFail to delete file {name}')
                    traceback.print_stack()      

    def add_arguments(self, parser):
        parser.add_argument("-c", "--check", dest="check", help="Check attachment")

    
    def handle(self, *args, **options):
        o_check = options.get("check")
        check = False
        if o_check and o_check.lower() == 'y':
            check = True

        self._process(check)
