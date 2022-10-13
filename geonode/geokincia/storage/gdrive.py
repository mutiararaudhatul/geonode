import os
from django.conf import settings
from .base import BaseStorage
import subprocess
import re
import logging

logger = logging.getLogger(__name__)

PERMISSION = {'r': 'reader', 'w': 'writer'}

class GDriveStorage(BaseStorage):
    def _execute_command(self, command, workdir, propagate=True):
        logger.debug(f'_execute_command {command}')
        ext = subprocess.run(command, capture_output=True, cwd=workdir, shell=True)
        logger.debug(f'_execute_command {ext}')
        if ext.returncode != 0 and propagate:
            raise Exception
        return ext

    def create_folder(self, folder_name):
        folder_name = 'upload/' + folder_name
        self._execute_command('drive new -folder %s' % folder_name, cwd=settings.GEOKINCIA['WORKING_DIR'])
        ext = self._execute_command('drive url %s' % folder_name, settings.GEOKINCIA['WORKING_DIR'])
        url = re.split(r'\s+', ext.stdout.decode('utf-8').strip())[1]
        os.makedirs(os.path.join(settings.GEOKINCIA['WORKING_DIR'], folder_name))
        return url

    def upload_file(self, local_file):
        local_file = 'source/' + local_file
        self._execute_command('drive push -no-prompt -quiet %s' % local_file, settings.GEOKINCIA['WORKING_DIR'])
        ext = self._execute_command('drive url %s' % local_file, settings.GEOKINCIA['WORKING_DIR'])
        url = re.split(r'\s+', ext.stdout.decode('utf-8').strip())[1]
        return url

    def share_file(self, local_file, users, permission, message = ''):
        if users:
            self._execute_command('drive share -no-prompt -quiet -type user -emails %s -role %s -message "%s" %s' % \
                (','.join(users), PERMISSION[permission], message, local_file), settings.GEOKINCIA['WORKING_DIR'])
        else:
            self._execute_command('drive share -no-prompt -quiet -type anyone -role %s %s' % \
                (PERMISSION[permission], local_file), settings.GEOKINCIA['WORKING_DIR'])

    def delete_file(self, name):
        self._execute_command('yes | drive delete -quiet %s' % name, settings.GEOKINCIA['WORKING_DIR'])

    def download_file(self, name):
        self._execute_command('drive pull -no-prompt -quiet %s' % name, settings.GEOKINCIA['WORKING_DIR'])

    def create_webhook(self, id):
        pass
        

        '''
        <?xml version="1.0" encoding="UTF-8"?><wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:dwn="http://geoserver.org/wps/download" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd"><ows:Identifier>gs:Download</ows:Identifier><wps:DataInputs><wps:Input><ows:Identifier>layerName</ows:Identifier><wps:Data><wps:LiteralData>geonode:ds1</wps:LiteralData></wps:Data></wps:Input><wps:Input><ows:Identifier>outputFormat</ows:Identifier><wps:Data><wps:LiteralData>application/zip</wps:LiteralData></wps:Data></wps:Input><wps:Input><ows:Identifier>targetCRS</ows:Identifier><wps:Data><wps:LiteralData>EPSG:4326</wps:LiteralData></wps:Data></wps:Input><wps:Input><ows:Identifier>cropToROI</ows:Identifier><wps:Data><wps:LiteralData>false</wps:LiteralData></wps:Data></wps:Input></wps:DataInputs><wps:ResponseForm><wps:ResponseDocument storeExecuteResponse="true" status="true"><wps:Output mimeType="application/zip" asReference="true"><ows:Identifier>result</ows:Identifier></wps:Output></wps:ResponseDocument></wps:ResponseForm></wps:Execute>
        '''