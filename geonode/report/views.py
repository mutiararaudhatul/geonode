from django.shortcuts import render, redirect, get_object_or_404
from .forms import LaporForm
from .models import Lapor
from django.core.files.storage import FileSystemStorage
from django.contrib import messages

def home(request):
    return render(request, 'home.html')

def pelaporan(request):
    if request.method == 'POST':
        form = LaporForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('pelaporan') 
    else:
        form = LaporForm()
    return render(request, 'pelaporan.html', {'form': form})

def handle_form_lapor(request):
    if request.method == 'POST':
        pelapor = request.POST['pelapor']
        nik = request.POST['nik']
        email = request.POST['email']
        nohp = request.POST['nohp']
        jenis_pelaporan = request.POST['jenis_pelaporan']
        lokasi = request.POST['lokasi']
        latitude = request.POST['latitude']
        longitude = request.POST['longitude']
        pesan = request.POST['pesan']
        foto_files = request.FILES.getlist('foto')
        
        pelaporan = Lapor.objects.create(
            pelapor=pelapor,
            nik=nik,
            email=email,
            nohp=nohp,
            jenis_pelaporan=jenis_pelaporan,
            lokasi=lokasi,
            latitude=latitude,
            longitude=longitude,
            pesan=pesan,
        )

        for foto in foto_files:
            fs = FileSystemStorage()
            filename = fs.save(foto.name, foto)
            pelaporan.fotos.create(foto=filename)

        messages.success(request, 'Terima kasih. Laporan anda sudah terkirim')
        return redirect('pelaporan')

    return render(request, 'pelaporan.html')

def laporan_list(request):
    lapors = Lapor.objects.all() 
    return render(request, 'laporan_list.html', {'lapors': lapors})


def laporan_detail(request, id):
    laporan = get_object_or_404(Lapor, id=id)
    return render(request, 'laporan_detail.html', {'laporan': laporan})
