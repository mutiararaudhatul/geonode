# Generated by Django 5.0.6 on 2024-07-01 05:38

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Lapor',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('pelapor', models.CharField(max_length=100)),
                ('nik', models.CharField(max_length=16)),
                ('email', models.EmailField(max_length=254)),
                ('nohp', models.CharField(max_length=15)),
                ('jenis_pelaporan', models.CharField(choices=[('Jalan Lingkungan', 'Jalan Lingkungan'), ('Drainase Lingkungan', 'Drainase Lingkungan'), ('Rumah Tidak Layak Huni (RTLH)', 'Rumah Tidak Layak Huni (RTLH)'), ('etc', 'etc')], max_length=50)),
                ('lokasi', models.CharField(max_length=255)),
                ('latitude', models.FloatField()),
                ('longitude', models.FloatField()),
                ('pesan', models.TextField()),
                ('status', models.CharField(choices=[('New', 'New'), ('Received', 'Received'), ('Refused', 'Refused'), ('Processing', 'Processing'), ('Completed', 'Completed')], default='New', max_length=15)),
                ('tanggal_pelaporan', models.DateTimeField(default=django.utils.timezone.now)),
            ],
        ),
        migrations.CreateModel(
            name='Foto',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('foto', models.ImageField(upload_to='photos/')),
                ('lapor', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='fotos', to='report.lapor')),
            ],
        ),
    ]
