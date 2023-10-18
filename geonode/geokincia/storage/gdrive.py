import os
from django.conf import settings
from .base import BaseStorage
import subprocess
import re
import logging

logger = logging.getLogger(__name__)

PERMISSION = {'r': 'reader', 'w': 'writer'} 

class GDriveStorage(BaseStorage):
    def __init__(self, name='', cwd='') -> None:
        super().__init__()
        self.name = name
        self.cwd = cwd

    def _execute_command(self, command, workdir, propagate=True):
        logger.debug(f'_execute_command {command}')
        ext = subprocess.run(command, capture_output=True, cwd=workdir, shell=True)
        logger.debug(f'_execute_command {ext}')
        if ext.returncode != 0 and propagate:
            raise Exception
        return ext

    def create_folder(self, folder_name):
        folder_name = 'upload/' + folder_name
        self._execute_command('drive new -folder %s' % folder_name, self.cwd)
        ext = self._execute_command('drive url %s' % folder_name, self.cwd)
        url = re.split(r'\s+', ext.stdout.decode('utf-8').strip())[1]
        try:
            os.makedirs(os.path.join(self.cwd, folder_name))
        except:
            pass
        return url

    def upload_file(self, local_file):
        local_file = 'source/' + local_file
        self._execute_command('drive push -no-prompt -quiet %s' % local_file, self.cwd)
        ext = self._execute_command('drive url %s' % local_file, self.cwd)
        url = re.split(r'\s+', ext.stdout.decode('utf-8').strip())[1]
        return url

    def share_file(self, local_file, users, permission, message = ''):
        if users:
            self._execute_command('drive share -no-prompt -quiet -type user -emails %s -role %s -message "%s" %s' % \
                (','.join(users), PERMISSION[permission], message, local_file), self.cwd)
        else:
            self._execute_command('drive share -no-prompt -quiet -type anyone -role %s %s' % \
                (PERMISSION[permission], local_file), self.cwd)

    def delete_file(self, name):
        self._execute_command('yes | drive delete -quiet %s' % name, self.cwd)

    def download_file(self, name):
        self._execute_command('drive pull -no-prompt -quiet -ignore-name-clashes %s' % name, self.cwd)

    def rename(self, old, new):
        self._execute_command('drive rename  -quiet %s %s' % (old, new), self.cwd)
        
