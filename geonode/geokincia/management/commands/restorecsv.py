import html
import re
from geonode.geokincia import db_utils
import csv, sys
from datetime import datetime
import os
from PIL import Image, ImageOps
from PIL.ExifTags import TAGS

FIELDS = ["geom","id","no_ruas","nama","peng_pangk","peng_ujung","fungsi","kecamatan","lebar_perk","lebar_jln",
          "pal_awal","pal_akhir","pal_bag","thn_update","konstruksi","kondisi","panjang","x","y","keterangan",
          "created_by","created_at","updated_by","updated_at","___id","___update","___att","lastupdate","ket2","___url_att"]
DIR = '/mnt/volumes/statics/static/attachment'
list_dir = os.listdir(DIR)
def check_and_fix_att(attrs):
    atts = attrs.split(';')
    new_atts = []
    
    for att in atts:
        new_att=''
        if att:
            detail_att = att.split('#')
            try:
                if detail_att[0].lower().endswith('.jpg'):
                    new_att = f'{detail_att[0]}#photo'
                elif detail_att[0].lower().endswith('.mp4'):
                    new_att = f'{detail_att[0]}#video'
                elif len(detail_att[0]) > 0:
                    raise Exception
            except IndexError:
                continue
            except:
                probably_files = list(filter(lambda d: d.startswith(detail_att[0]), list_dir))
                if len(probably_files) == 1:
                    detail_att[0] = probably_files[0]
                    if detail_att[0].lower().endswith('.jpg'):
                        new_att = f'{detail_att[0]}#photo'
                    elif detail_att[0].lower().endswith('.mp4'):
                        new_att = f'{detail_att[0]}#video'
                else:
                    continue
            try:
                tgl = datetime.strptime(detail_att[2], '%a %d-%m-%Y')
                if tgl > datetime(2000,1,1):
                    new_att += f'#{detail_att[2]}'
                else:
                    raise Exception
            except:
                dd  = None
                if (new_att.endswith('#photo')):
                    with Image.open(DIR + '/' + detail_att[0]) as img:
                        ext = img.getexif()
                        try:
                            dd =  datetime.strptime(ext[306], '%Y:%m:%d %H:%M:%S')
                        except FileNotFoundError:
                            raise FileNotFoundError
                        except:
                            dd =  datetime.now()
                else:
                    stat = os.stat(DIR + '/' + detail_att[0])
                    dd = datetime.fromtimestamp(stat.st_atime)
                if dd:
                    new_att += '#' + dd.strftime("%a %d-%m-%Y")
            new_atts.append(new_att)
    return ';'.join(new_atts)


not_exist = 0
exist = 0
def main(source, target, datastore):
    global exist, not_exist
    with open(source, 'r', encoding='unicode_escape') as f:
        csv_shp = csv.reader(f, dialect='excel')
        header = next(csv_shp)
        for row in csv_shp:
            e = db_utils.execute_query(datastore, f'select id from "JALAN_LINGKUNGAN" where id ={row[1]}', None, True)
            if len(e) > 0:
                exist +=1
            else:
                not_exist += 1
                att = check_and_fix_att(row[26])
                #insert
                row[26] = att
                values_txt = ','.join(['null' if not v else "'%s'" % html.escape(re.sub(r"'", "''", v), quote=False) for v in row[1:]])
                
               
                values_txt = "st_multi(st_astext(st_force2d('%s'))), %s" % (row[0], values_txt)
                sql = '''insert into "%s" ("geom","id","no_ruas","nama","peng_pangk","peng_ujung","fungsi","kecamatan","lebar_perk","lebar_jln","pal_awal","pal_akhir","pal_bag","thn_update","konstruksi","kondisi","panjang","x","y","keterangan","created_by","created_at","updated_by","updated_at","___id","___update","___att","lastupdate","ket2","___url_att") 
                values(%s)''' % (target,  values_txt)
                
                #db_utils.execute_query(datastore, sql, None, False)
                print(sql)

# if __name__ == "__main__":
main('/backup_restore/j/source/jaling0.csv', 'JALAN_LINGKUNGAN_TEMP', 'datastore')
print(f'exist: {exist} non: {not_exist}')
                

