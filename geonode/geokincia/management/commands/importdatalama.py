from django.core.management.base import BaseCommand
from geonode.geokincia import db_utils

import os
import subprocess
import csv
import uuid

class Command(BaseCommand):
    help = (
            "Import data lama ke layer yang sudah ada field foto/video: Foto, Foto1, Foto2, FotoX,... Video, Video1,Video2,..."
        )

    def _process_csv(self, source, target, att_path):
        rows = []
        with open(source, 'r') as f:
            reader = csv.reader(f, dialect='excel')
            header = next(reader)
            for r in reader:
                rows.append(r)
        fields = []
        for i, h in enumerate(header):
            h_lower = h.lower()
            if h_lower.startswith('foto') or h_lower.startswith('video'):
                fields.append(i)
        for field in fields:
            header[field : field + 1] = []
        header.append('Attachments')
        header.append('___id')
        header.append('___update')
        header.append('___att')
        for row in rows:
            new_att = []
            for field in fields:
                if row[field]:
                    row[field] = row[field].replace('\\','/')
                    try:
                        row[field] = row[field][row[field].index(':')+2:]
                    except:
                        pass
                    print(row[field])
                    if not os.path.exists(os.path.join(att_path, row[field])):
                        print(f'file {row[field]} does not exist')
                    new_att.append(os.path.join(att_path, row[field]))
            for field in fields:
                row[field:field+1] = []
            row.append(','.join(new_att))
            row.append(str(uuid.uuid4()))
            row.append(None)
            row.append(None)
        with open(os.path.join(os.path.dirname(source), '___out2.csv'), 'w') as f:
            writer = csv.writer(f, dialect='excel')
            writer.writerow(header)
            for row in rows:
                writer.writerow(row)

        db_utils.load_from_csv('datastore', os.path.join(os.path.dirname(source), '___out2.csv'), target, False, None, True)

    def _process_shp(self, source, target, att_path):
        if os.path.exists(source + '.csv'):
            os.remove(source + '.csv')
        r = subprocess.run(f'ogr2ogr -f CSV -overwrite {os.path.basename(source)}.csv  {os.path.basename(source)} -lco GEOMETRY=AS_WKT -t_srs "EPSG:4326" ', capture_output=True, shell=True, cwd=os.path.dirname(source))
        if r.returncode != 0:
            print(f'Gagal process file {source}')
            return
        self._process_csv(os.path.join(os.path.dirname(source), os.path.basename(source) + '.csv'), target, att_path)

    def add_arguments(self, parser):
        parser.add_argument("-t", "--target", dest="target", help="Name layer target.")

        parser.add_argument("-s", "--source", dest="source", help="File sumber shp/csv.")

    
    def handle(self, *args, **options):
        target = options.get("target")
        source = options.get("source")
        path = '.'

        if not target or not source:
            self.print_help('manage.py', 'importdatalama')
            return
        
        if not os.path.exists(source):
            print('source not found')
            return
        
        if source.lower().endswith('.shp'):
            self._process_shp(source, target, path)
        elif source.lower().endswith('.csv'):
            self._process_csv(source, target, path)
