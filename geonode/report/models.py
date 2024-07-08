from django.db import models
from django.utils import timezone

class Lapor(models.Model):
    STATUS_CHOICES = (
        ('Baru_Masuk', 'Baru Masuk'),
        ('Diterima', 'Diterima'),
        ('Ditolak', 'Ditolak'),
        ('Diproses', 'Diproses'),
        ('Selesai', 'Selesai')
    )

    JENIS_CHOICES = (
        ('Jalan Lingkungan', 'Jalan Lingkungan'),
        ('Drainase Lingkungan', 'Drainase Lingkungan'),
        ('Rumah Tidak Layak Huni (RTLH)', 'Rumah Tidak Layak Huni (RTLH)'),
        ('etc', 'etc')
    )

    pelapor = models.CharField(max_length=100)
    nik = models.CharField(max_length=16)
    email = models.EmailField()
    nohp = models.CharField(max_length=15)
    jenis_pelaporan = models.CharField(max_length=50, choices=JENIS_CHOICES)
    lokasi = models.CharField(max_length=255)
    latitude = models.FloatField()
    longitude = models.FloatField()
    pesan = models.TextField()
    status = models.CharField(max_length=15, choices=STATUS_CHOICES, default='Baru_Masuk')
    tanggal_pelaporan = models.DateTimeField(default=timezone.now)  

    class Meta:
        app_label = 'report'

class Foto(models.Model):
    lapor = models.ForeignKey(Lapor, related_name='fotos', on_delete=models.CASCADE)
    foto = models.ImageField(upload_to='photos/')

    class Meta:
        app_label = 'report'
