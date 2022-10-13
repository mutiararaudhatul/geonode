from distutils.command.upload import upload
from genericpath import isdir
import shutil
from django.conf import settings
from django.db import connections
from django.db.models import Q
from django.core.mail import send_mail
from django.contrib.auth import get_user_model
from geonode.layers.models import Dataset, UserCollectorStorage
from geonode.celery_app import app
from celery.utils.log import get_task_logger
from datetime import datetime

from . import utils

import os

logger = get_task_logger(__name__)

@app.task(
    bind=True,
    name='geokincia.dataset.prepare',
    queue='cleanup',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False)
def prepare_dataset_task(self, dataset_id, reupload=False):
    try:
        layer = Dataset.objects.get(id=dataset_id)
        user_collectors = UserCollectorStorage.objects.filter(dataset=layer)
    except Dataset.DoesNotExist or UserCollectorStorage.DoesNotExist:
        logger.warning(f"Layers {dataset_id} or collector does not exist!")
        return
    
    if not layer.intermediate_storage:
        logger.warning(f'No intermediate storage been choosen on {dataset_id}')
        return
    storage_config = settings.GEOKINCIA['STORAGE'][layer.intermediate_storage]
    storage = utils.get_class(storage_config['CLASS_NAME'])

    users = set()
    admins = set()
    for admin in get_user_model().objects.filter(Q(is_superuser=True) | Q(is_staff=True), is_active=True):
        users.add(admin.email)
        admins.add(admin.email)
    if layer.owner:
        users.add(layer.owner.email)
    if layer.group:
        for user_group in get_user_model().objects.filter(groups__in=[layer.group.id]):
            users.add(user_group.email)
    for u in user_collectors:
        users.add(u.user.email)
    #src
    if not layer.source_url:
        reupload = True
    if reupload:
        try:
            if layer.source_url:
                logger.debug(f'delete {layer.file_path}')
                storage.delete_file(layer.file_path)
                try:
                    os.remove(os.path.join(settings.GEOKINCIA['WORKING_DIR'], layer.file_path))
                except:
                    pass
            source_file = utils.download_source_dataset(layer.workspace, layer.name)
            logger.debug(f'')
            source_url = storage.upload_file(os.path.basename(source_file))
            layer.source_url = source_url
            layer.file_path = os.path.join('source', os.path.basename(source_file))
            storage.share_file(layer.file_path, users, 'r', 'Dataset sumber project:' + layer.title)
            layer.save()
        except:
            logger.error(f'fail to update source dataet {dataset_id}')
            send_mail(f'Project {layer.title}: Gagal Buat Source Dataset',
                    f'Gagal membuat  source dataset untuk project {layer.title}',None, admins)
    #upload
    for user_collector in user_collectors:
        if not user_collector.upload_url:
            try:
                folder = layer.name + '_' + str(layer.id) + '/' + user_collector.user.username
                upload_url = storage.create_folder(folder)
                user_collector.upload_url = upload_url
                user_collector.folder = 'upload/' + folder
                storage.share_file(folder, None, 'w')
                user_collector.save()
                send_mail('Folder Upload Project %s' % layer.title,
'''Berikut adalah folder upload untuk project %s user %s .
%s
Sebelum me-upload silahkan buat shortcut ke 'home' anda''' % (layer.title, user_collector.user.username, upload_url)
            ,None, users)
            except:
                logger.warning(f'fail to create upload folder for user {user_collector.user.username} dataset {dataset_id}')
                send_mail(f'Project {layer.title}: Gagal Buat Upload Folder',
                f'Gagal membuat upload folder untuk project {layer.title} user {user_collector.user.username}',None, admins)
            

@app.task(
    bind=True,
    name='geokincia.dataset.upload',
    queue='cleanup',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False)
def process_uploaded_data_task(self, storage_provider):
    upload_dir = os.path.join(settings.GEOKINCIA['WORKING_DIR'], 'upload')
    error_dir = os.path.join(settings.GEOKINCIA['WORKING_DIR'], 'processed', 'error')
    succes_dir = os.path.join(settings.GEOKINCIA['WORKING_DIR'], 'processed', 'success')
    
    storage_config = settings.GEOKINCIA['STORAGE'][storage_provider]
    storage = utils.get_class(storage_config['CLASS_NAME'])

    admins = set()
    for admin in get_user_model().objects.filter(Q(is_superuser=True) | Q(is_staff=True), is_active=True):
        admins.add(admin.email)

    try:
        storage.download_file('upload')
    except:
        logger.warning(f'fail to pull uploaded dataset')
        send_mail(f'{storage_provider} Pull dataset gagal ',
                f'{storage_provider} Pull dataset gagal',None, admins)

    for layer_dir in os.listdir(upload_dir):
        for user_dataset in os.listdir(os.path.join(upload_dir, layer_dir)):
            for uploaded in os.listdir(os.path.join(upload_dir, layer_dir, user_dataset)):
                if os.path.isdir(os.path.join(upload_dir, layer_dir, user_dataset, uploaded)):
                    uploaded_path = os.path.join(upload_dir, layer_dir, user_dataset, uploaded)
                    try:
                        csv_shp = list(filter(lambda f: f.lower().endswith('.csv') or f.lower().endswith('.shp'), os.listdir(uploaded_path)))
                        processed_file = csv_shp.pop() if csv_shp else ''
                        if os.path.isfile(os.path.join(uploaded_path), processed_file):
                            if processed_file.lower().endswith('.csv'):
                                utils.process_csv(os.path.join(uploaded_path, processed_file), user_dataset, int(layer_dir.split('_')[-1]))
                            elif processed_file.lower().endswith('.shp'):
                                utils.process_shp(os.path.join(uploaded_path, processed_file), user_dataset, int(layer_dir.split('_')[-1]))
                                
                        user_success_dir = os.path.join(succes_dir, user_dataset, str(int(datetime.now().timestamp())))
                        os.makedirs(user_success_dir)
                        shutil.move(uploaded_path, user_success_dir)
                        storage.delete_file(f'upload/{layer_dir}/{user_dataset}/{uploaded}')
                    except:
                        user_error_dir = os.path.join(error_dir, user_dataset, str(int(datetime.now().timestamp())))
                        os.makedirs(user_error_dir)
                        shutil.move(uploaded_path, user_error_dir)
                        logger.warning(f'fail to pull uploaded dataset')
                        send_mail(f'{storage_provider} Pull dataset gagal ',
                                f'{storage_provider} Pull dataset gagal',None, admins)

@app.task(
    bind=True,
    name='geokincia.dataset.delete',
    queue='cleanup',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False)
def delete_file_task(self, storage_provider, filename):
    storage_config = settings.GEOKINCIA['STORAGE'][storage_provider]
    storage = utils.get_class(storage_config['CLASS_NAME'])
    storage.delete_file(filename)
    f_path = os.path.join(settings.GEOKINCIA['WORKING_DIR'], filename)
    try:
        if os.path.isdir(f_path):
            shutil.rmtree(f_path)
        else:
            os.remove(f_path)
    except:
        pass