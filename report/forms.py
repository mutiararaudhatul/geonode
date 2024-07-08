from django import forms
from .models import Lapor

class LaporForm(forms.ModelForm):
    latitude = forms.FloatField(widget=forms.HiddenInput())
    longitude = forms.FloatField(widget=forms.HiddenInput())

    class Meta:
        model = Lapor
        fields = ['pelapor', 'nik', 'email', 'nohp', 'jenis_pelaporan', 'lokasi', 'pesan', 'tanggal_pelaporan', 'latitude', 'longitude', 'status']
        widgets = {
            'jenis_pelaporan': forms.Select(choices=[
                ('Jalan Lingkungan', 'Jalan Lingkungan'),
                ('Drainase Lingkungan', 'Drainase Lingkungan'),
                ('Rumah Tidak Layak Huni (RTLH)', 'Rumah Tidak Layak Huni (RTLH)'),
                ('dst', 'dst'),
            ]),
        }