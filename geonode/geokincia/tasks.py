from distutils.command.upload import upload
from genericpath import isdir
import shutil
from django.conf import settings
from django.db import connections
from django.db.models import Q
from geonode.tasks.tasks import send_email
from django.contrib.auth import get_user_model
from geonode.geokincia import db_utils
from geonode.layers.models import Dataset, UserCollectorStorage, UserStorage
from geonode.base.models import Configuration
from geonode.layers.tasks import delete_dataset
from geonode.celery_app import app
from datetime import datetime
from pathlib import Path

import logging
from . import utils

import os
import traceback
import tempfile
import zipfile

logger = logging.getLogger(__name__)

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
    storage.cwd = storage_config['WORKING_DIR']
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
                    os.remove(os.path.join(storage_config['WORKING_DIR'], layer.file_path))
                except:
                    pass
            source_file = utils.download_source_dataset(layer.workspace, layer.name, storage_config['WORKING_DIR'])
            logger.debug(f'source_file')
            source_url = storage.upload_file(os.path.basename(source_file))
            layer.source_url = source_url
            layer.file_path = os.path.join('source', os.path.basename(source_file))
            storage.share_file(layer.file_path, None, 'r')
            layer.save()
            send_email(f'Source Dataset Project %s' % layer.title,
                f'Berikut adalah source dataset untuk project {layer.title} : \n {source_url} \n' +
                'Sebelum me-upload silahkan buat shortcut ke home anda', 
                settings.DEFAULT_FROM_EMAIL, users, fail_silently=True)
        except:
            logger.error(f'fail to update source dataet {dataset_id}')
            logger.debug(traceback.format_exc())
            send_email(f'Project {layer.title}: Gagal Buat Source Dataset',
                    f'Gagal membuat  source dataset untuk project {layer.title}', 
                    settings.DEFAULT_FROM_EMAIL, admins, fail_silently=True)
    #upload
    logger.debug(f'user_collectors {len(user_collectors)}')
    for user_collector in user_collectors:
        logger.debug(f'user_collector {user_collector.user.username}')
        us = UserStorage.objects.filter(user=user_collector.user, dataset=user_collector.dataset)
        if len(us) < 1:
        #if not user_collector.upload_url:
            try:
                folder = layer.name + '_' + str(layer.id) + '/' + layer.name + '_' + user_collector.user.username
                upload_url = storage.create_folder(folder)
                storage.share_file('upload/' + folder, None, 'w')
                # user_collector.upload_url = upload_url                
                # user_collector.folder = 'upload/' + folder
                # user_collector.save()
                new_us = UserStorage()
                new_us.user = user_collector.user
                new_us.dataset = user_collector.dataset
                new_us.upload_url = upload_url
                new_us.folder = 'upload/' + folder
                new_us.save()
                send_email(f'Folder Upload Project {layer.title}',
                    f'Berikut adalah folder upload untuk project {layer.title} user {user_collector.user.username} :\n' +
                    f'{upload_url} \nSebelum me-upload silahkan buat shortcut ke home anda',
                    settings.DEFAULT_FROM_EMAIL, [user_collector.user.email], fail_silently=True)
            except:
                logger.warning(f'fail to create upload folder for user {user_collector.user.username} dataset {dataset_id}')
                logger.debug(traceback.format_exc())
                send_email(f'Project {layer.title}: Gagal Buat Upload Folder',
                f'Gagal membuat upload folder untuk project {layer.title} user {user_collector.user.username}', settings.DEFAULT_FROM_EMAIL, admins, fail_silently=True)

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
    storage_config = settings.GEOKINCIA['STORAGE'][storage_provider]
    storage = utils.get_class(storage_config['CLASS_NAME'])
    storage.cwd = storage_config['WORKING_DIR']
    upload_dir = os.path.join(storage_config['WORKING_DIR'], 'upload')
    remote_dir = 'upload'
    admins = set()
    for admin in get_user_model().objects.filter(Q(is_superuser=True) | Q(is_staff=True), is_active=True):
        admins.add(admin.email)

    ds_collectors = Dataset.objects.filter(is_data_collector=True)
    ds_folder_names = [f'{ds.name}_{ds.id}' for ds in ds_collectors]
    for layer_dir in os.listdir(upload_dir):
        if layer_dir not in ds_folder_names:
            continue
        ds_users = [f'{uc.dataset.name}_{uc.username}' for uc in UserCollectorStorage.objects.filter(Dataset__id=layer_dir.split('_')[-1])]
        for user_dataset in os.listdir(os.path.join(upload_dir, layer_dir)):
            if user_dataset not in ds_users:
                continue
            uploaded_list = os.listdir(os.path.join(upload_dir, layer_dir, user_dataset))
            for uploaded in uploaded_list:
                remote_path = os.path.join(remote_dir, layer_dir, user_dataset, uploaded)
                uploaded_path = os.path.join(upload_dir, layer_dir, user_dataset, uploaded)
                if uploaded.endswith('.zip'):
                    logger.info(f'found zip: {uploaded}')
                    try:
                        tempdir = tempfile.mkdtemp()
                        zf = zipfile.ZipFile(uploaded_path)
                        zf.extractall(tempdir)
                        for ef in os.listdir(tempdir):
                            sdir = os.path.join(tempdir, ef)
                            logger.info(f'extracted file: {ef}')
                            target = ef + '___extracted'
                            if os.path.isdir(sdir) and not os.path.exists(os.path.join(upload_dir, layer_dir, user_dataset, target)):
                                shutil.copytree(sdir, os.path.join(upload_dir, layer_dir, user_dataset, target))
                                uploaded_list.append(target)
                        shutil.rmtree(tempdir)
                        storage.rename(remote_path, f'"___processed_{uploaded}_{int(datetime.now().timestamp())}_success"')
                        continue
                    except:
                        if os.path.exists(tempdir):
                            shutil.rmtree(tempdir)
                        logger.debug(traceback.format_exc())
                        if os.path.exists(uploaded_path + '.control'):
                            with open(uploaded_path + '.control', 'r') as cf:
                                control = int(cf.readline().strip())
                                td = datetime.now() - datetime.fromtimestamp(control)
                                if td.total_seconds() > int(settings.GEOKINCIA['MAX_SECONDS_DOWNLOAD_WAIT']):
                                    storage.rename(remote_path, f'"___processed_{uploaded}_{int(datetime.now().timestamp())}_error"')
                        else:
                            with open(uploaded_path + '.control', 'w') as cf:
                                cf.write(str(int(datetime.now().timestamp())))

                if not uploaded.startswith('___processed_') and os.path.isdir(uploaded_path):
                    try:
                        csv = list(filter(lambda f: f.lower().endswith('.csv'), os.listdir(uploaded_path)))
                        if len(csv) > 0:
                            csv_file = os.path.join(uploaded_path, csv[0])
                            logger.info(f'csv {csv_file}')
                        else:
                            shp = list(filter(lambda f: f.lower().endswith('.shp'), os.listdir(uploaded_path)))
                            if len(shp) > 0:
                                utils.process_shp(os.path.join(uploaded_path, shp[0]))
                                logger.info(f'shp {shp[0]}')
                                csv_file = os.path.join(uploaded_path, 'out.csv')
                                logger.info(f'shp to csv {csv_file}')
                            else:
                                #download not complte. skipping
                                continue

                        if os.path.exists(os.path.join(uploaded_path, '.gn-timecheck')):
                            with open(os.path.join(uploaded_path, '.gn-timecheck'), 'r') as cf:
                                try:
                                    control = int(cf.readline().strip())
                                    td = datetime.now() - datetime.fromtimestamp(control)
                                    if td.total_seconds() < int(settings.GEOKINCIA['MAX_SECONDS_DOWNLOAD_WAIT']):
                                        utils.all_attachment_exists(csv_file)
                                except:
                                    #utils.add_time_check(csv_file)
                                    continue
                        else:
                            try:
                                utils.all_attachment_exists(csv_file)
                            except:
                                utils.add_time_check(csv_file)
                                continue

                        utils.process_csv(csv_file, user_dataset.split('_')[-1], int(layer_dir.split('_')[-1]))
                        if uploaded.endswith('___extracted'):
                            shutil.rmtree(uploaded_path)
                        else:
                            storage.rename(remote_path, f'"___processed_{uploaded}_{int(datetime.now().timestamp())}_success"')
                    except:
                        logger.warning(f'fail to processed uploaded dataset')
                        logger.debug(traceback.format_exc())
                        try:
                            if uploaded.endswith('___extracted'):
                                shutil.rmtree(uploaded_path)
                            else:
                                storage.rename(remote_path, f'"___processed_{uploaded}_{int(datetime.now().timestamp())}_error"')
                            send_email(f'{storage_provider} Pull dataset gagal ',   
                                f'{storage_provider} Pull dataset gagal', settings.DEFAULT_FROM_EMAIL, admins, fail_silently=True)
                        except:
                            logger.debug(f'Fail to delete or rename error')
                        

@app.task(
    bind=True,
    name='geokincia.dataset.check_upload',
    queue='cleanup')
def check_and_process_data_taskk(self):
    config = Configuration.objects.all()[0]
    if config.read_only or config.maintenance:
        return
    if not os.path.exists('.check_lock'):
        Path('.check_lock').touch()
    else:
        logger.debug('Checker already running')
        return
    
    storage_config = settings.GEOKINCIA['STORAGE']
    for provider in storage_config.values():
        storage = utils.get_class(provider['CLASS_NAME'])
        storage.cwd = provider['WORKING_DIR']

        try:
            storage.download_file('upload')
        except:
            logger.warning(f'fail to pull uploaded dataset')
            logger.debug(traceback.format_exc())

    try:
        for provider in storage_config.keys():
            logger.debug(f'schedule checking: {provider}')
            if UserCollectorStorage.objects.filter(dataset__intermediate_storage=provider).count() > 0:
                process_uploaded_data_task(provider)

        os.remove('.check_lock')
    except:
        pass

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
    storage.cwd = storage_config['WORKING_DIR']
    try:
        storage.delete_file(filename)
    except:
        pass
    try:
        f_path = os.path.join(storage_config['WORKING_DIR'], filename)
    
        if os.path.isdir(f_path):
            shutil.rmtree(f_path)
        else:
            os.remove(f_path)
    except:
        logger.debug(traceback.format_exc())
        pass

@app.task(
    bind=True,
    name='geokincia.dataset.merge',
    queue='cleanup',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False)
def merge_dataset_task(self, dataset_id, uc_dataset):
    try:
        layer = Dataset.objects.get(id=dataset_id)
    except Dataset.DoesNotExist:
        logger.warning(f"Layers {dataset_id} or collector does not exist!")
        return
    for intermediate_dataset_name in uc_dataset:
        if intermediate_dataset_name:
            try:
                db_utils.copy_table('datastore', intermediate_dataset_name, layer.name)
                delete_dataset.delay(intermediate_dataset_name)
            except:
                logger.debug(traceback.format_exc())
    utils.truncate_geoserver_cache(layer.workspace, layer.name)

