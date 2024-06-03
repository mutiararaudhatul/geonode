# Generated by Django 3.2.4 on 2021-07-20 11:05

from django.conf import settings
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('services', '0045_auto_20210629_1355'),
        ('upload', '0033_auto_20210531_1252'),
        ('auth', '0012_alter_user_first_name_max_length'),
        ('harvesting', '0026_harvestableresource_last_harvesting_succeeded'),
        ('base', '0068_rename_storetype_resourcebase_subtype'),
        ('contenttypes', '0002_remove_content_type_name'),
        ('layers', '0037_layer_ptype'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='Layer',
            new_name='Dataset',
        ),
    ]