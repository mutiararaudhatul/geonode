from django.db import connections
from django.conf import settings
from django.contrib.auth import get_user_model
from datetime import datetime, timedelta
from PIL import Image, ImageOps

import html
import re
import csv
import os
import shutil
import uuid
import logging
import base64

ATTACHMENT = ('attachment', 'Attachment', 'attachments', 'Attachments')
PHOTO_EXT = ['.png', '.jpg', '.jpeg', '.svg']
VIDEO_EXT = ['.mp4', '.avi']
IMG_SIZE = 720

logger = logging.getLogger(__name__)

def resize_image(src, target):
    if os.path.exists(target):
        return
    try:
        with Image.open(src) as img:
            img = ImageOps.exif_transpose(img)
            x, y = img.size
            ratio = IMG_SIZE / min(x,y)
            img = img.resize((int(x * ratio), int(y * ratio)))
            img.save(target, optimize=True, quality=90)
    except:
        shutil.copy2(src, target)

def process_attachment(attachment='', cwd='.', existing_attachment='', user=''):
    if not attachment or len(attachment.strip()) == 0:
         return existing_attachment
    new_attachments = []
    existing_attachments = []
    existing_attachments_files = []
    attachment_dir = settings.GEOKINCIA['ATTACHMENT_DIR']
    if not os.path.exists(attachment_dir):
        os.makedirs(attachment_dir)
    if existing_attachment and len(existing_attachment.strip()) > 0:
        existing_attachments = existing_attachment.strip().split(';')
        existing_attachments_files = [att.split('#')[0] for att in existing_attachments]
    for att in attachment.strip().split(','):
        origin = os.path.join(cwd, att.strip())
        base, f = os.path.split(origin)
        f = user + '_' + f
        target = os.path.join(attachment_dir, f)
        if not os.path.exists(origin):
            continue
        if not f in existing_attachments_files:
            f_prop = f
            _,ext = os.path.splitext(origin)
            if ext in PHOTO_EXT:
                f_prop += '#photo'
                resize_image(origin, target)
            elif ext in VIDEO_EXT:
                f_prop += '#video'
                if not os.path.exists(target):
                    shutil.copy2(origin, target)
            else:
                continue
            stat = os.stat(target)
            f_prop += f'#{datetime.fromtimestamp(stat.st_atime).strftime("%a %d-%m-%Y")}'
            new_attachments.append(f_prop)
    
    all_att = new_attachments + existing_attachments
    if settings.GEOKINCIA['MAX_ATTACHMENT'] > 0:
        all_att = all_att[:settings.GEOKINCIA['MAX_ATTACHMENT']]
    logger.debug(f'process_attachment {";".join(all_att)}')
    return ';'.join(all_att)


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def execute_query(conn_name, query, params, is_return, is_dict=True):
    '''Execute query'''
    logger.debug(f'q: {query}')
    with connections[conn_name].cursor() as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if is_return:
            if is_dict:
                return dictfetchall(cursor)
            else:
                return cursor.fetchall()

def empty_table(conn_name, table):
    q = f'delete from "{table}"'
    execute_query(conn_name, q, None, False)

def get_column_name(conn_name, table_name):
    q = '''select column_name, data_type from information_schema.columns where table_schema = 'public'
        and table_name= %s and data_type <> 'USER-DEFINED' '''
    return execute_query(conn_name, q, [table_name, ], True)

def get_geom_column(conn_name, table_name):
    q = '''select f_geometry_column, type, srid
        from geometry_columns
        where f_table_schema = 'public'
        and f_table_name = %s;'''

    return execute_query(conn_name, q, [table_name,], True)[0]

def get_primary_key(conn_name, table_name):
    q = '''SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '"%s"'::regclass
        AND    i.indisprimary;''' % table_name
    pr = execute_query(conn_name, q, None, True)
    return pr[0]['attname']


def insert_row(conn_name, table_name, colums, values, add_multi=None, target_geo='', geo_value=''):
    colums_txt = ','.join([ '"%s"' % c for c in colums])
    values_txt = ','.join(['null' if v is None else "'%s'" % html.escape(re.sub(r"'", "''", v), quote=False) for v in values[1:]])
    values_txt = "st_astext(st_force2d('%s')), %s" % (values[0], values_txt)
    if add_multi:
        q = 'insert into "%s"("%s", %s) values(st_multi(\'%s\'), %s)' % (table_name, target_geo, colums_txt, geo_value, values_txt)
    else:
        q = 'insert into "%s"(%s) values(%s)' % (table_name, colums_txt, values_txt)
    execute_query(conn_name,q, None, False)

def update_row(conn_name, table_name, columns, values, col_id, col_id_value, add_multi=None, target_geo='', geo_value='', force2d=False):
    update_values = []
    if force2d:
        irange = range(1, len(columns))
        update_values.append(f'"{columns[0]}"= st_astext(st_force2d(\'{values[0]}\'))')
    else:
        irange = range(len(columns))
    for i in irange:
        value = None
        if values[i] and (type(values[i]) == str or type(values[i]) == bytes):
            value = re.sub("'", "''", values[i])
            value = html.escape(html.unescape(value), quote=False)
        elif values[i]:
            value = values[i]
        value = f'"{columns[i]}"=null' if not values[i] else "\"%s\"='%s'" % (columns[i], value)
        update_values.append(value)

    q = "update \"%s\" set %s where \"%s\"='%s' " % (table_name, ','.join(update_values), col_id, col_id_value)
    if add_multi:
        q = q + '"%s" = st_multi(st\'%s\')' % (target_geo, geo_value)
    execute_query(conn_name, q, None, False)

def remove_primary(conn_name, table, columns):
    try:
        index_pr = columns.index(get_primary_key(conn_name, table))
        index_name = columns[index_pr]
        columns[index_pr:index_pr+1] = []
        return index_name, columns
    except:
        return -1, columns

def delete_table(conn_name, table):
    pass


def update_csv_geom(target_geom, src_geom, rows, index_geom):
    logger.debug(index_geom)
    for i in range(len(rows)):
        rows[i][index_geom] = re.sub(r'[Nn][Aa][Nn]', '0', rows[i][index_geom])
    def _to_multi(row):
        coord_pair = re.findall(r'(\([0-9.,() \-Nan]*\))', row[index_geom])[0]
        geo_type = re.findall(r'(LINESTRING|POINT|POLYGON)', row[index_geom])[0]
    
        row[index_geom] = 'MULTI' + geo_type + ' (' + coord_pair + ')'
        return row
    def _to_single(row):
        logger.debug(row)
        coord_pair = re.findall(r'(\([0-9.,() \-Nan]*\))', row[index_geom])[0]
        geo_type = re.findall(r'(LINESTRING|POINT|POLYGON)', row[index_geom])[0]
        
        row[index_geom] = geo_type + ' ' + coord_pair[1:len(coord_pair) -1]
        return row

    if target_geom[1] != src_geom[1]:
        logger.debug(f'geometry beda {target_geom[1]} {src_geom[1]}')
        raise Exception
    if target_geom[0]+target_geom[1] == src_geom[0]+src_geom[1]:
        return rows
    
    if target_geom[0] == 'MULTI' and src_geom[0] != 'MULTI':
        return list(map(_to_multi, rows))

    elif target_geom[0] != 'MULTI' and src_geom[0] == 'MULTI':
        return list(map(_to_single, rows))

def update_url_att(conn_name, primary_field, target_table):
    try:
        if not primary_field:
            primary_field = get_primary_key(conn_name, target_table)
        execute_query(conn_name,
                    f'''update "{target_table}" set "___url_att"='{settings.SITEURL}static/viewer/index.html?t={target_table}&n={primary_field}&i=' || {primary_field} 
                    where "___att" <> '' and ("___url_att" = '') is not false''', None, False, False)
    except:
        logger.debug(f'error updating url att')
        pass
    
def load_from_csv(conn_name, csv_file, target_table, is_sync, src_table=None, is_init=False, user=''):
    user_id = user
    if user:
        try:
            user_model = get_user_model().objects.get(username=user)
            user_id = str(user_model.id)
        except:
            pass

    rows = []
    header = []
    with open(csv_file, 'r') as f:
        processed_csv = csv.reader(f, dialect='excel')
        try:
            header = next(processed_csv)
        except:
            return
        for r in processed_csv:
            rows.append(r)
    if len(rows) < 1:
        return

    for i in range(len(rows)):
        rows[i] = [r if len(r) > 0 else None for r in rows[i]]

    index_geom = 0

    try:
        target_geo = get_geom_column(conn_name, target_table)
        target_geo_type = re.findall(r'(MULTI)*(\w+)', target_geo['type'], re.IGNORECASE)
        src_geo_type = re.findall(r'(MULTI)*(\w+)', rows[0][0], re.IGNORECASE)
        logger.debug(f'before row geo update: {rows[0][0]}')
        rows = update_csv_geom(target_geo_type[0], src_geo_type[0], rows, index_geom)

        logger.debug(f'after row geo update: {rows[0][0]}')
    except:
        raise Exception
    header[0] = target_geo['f_geometry_column']

    index_att = None
    name_att = ''
    for i in range(len(header)):
        if header[i] in ATTACHMENT:
            name_att = header[i]
            break

    target_columns = [c['column_name'] for c in get_column_name(conn_name, target_table)]
    remove_columns = []
    if name_att:
        target_columns.append(name_att)
    
    primary_field = None
    try:
        primary_field = get_primary_key(conn_name, target_table)
        remove_columns.append(header.index(primary_field))
    except:
        pass
    target_columns.insert(index_geom, header[0])
    for column in header:
        if not column in target_columns:
            remove_columns.append(header.index(column))
    logger.debug(repr(remove_columns))
    
    if len(remove_columns) > 2 or abs(len(target_columns) - len(header)) > 2:
        logger.debug(f'too many remove columns. probably wrong dataset')
        raise Exception

    rem_cols = ['___att', 'created_by', 'created_at', 'updated_at', 'updated_by', '___url_att']
    for col in rem_cols:
        try:
            remove_columns.append(header.index(col))
        except:
            pass
    remove_columns.sort()
    remove_columns.reverse()
    
    logger.debug(f'remove column : {remove_columns}')

    is_updated_at = False
    index_updated_at = -1
    index_id = header.index('___id')
    index_update = header.index('___update')
    date_compare = datetime.now() - timedelta(days=730)

    if name_att:
        index_att = header.index(name_att)

    if 'lastupdate' in header:
        is_updated_at = True
        index_updated_at = header.index('lastupdate')

    if is_init:
        inserted_rows = rows
        updated_rows = []
    else:
        empty_table(conn_name, target_table)

        if is_sync:
            inserted_rows = list(filter(lambda r: not r[index_id], rows))
            if is_updated_at:
                updated_rows = list(filter(lambda r: r[index_id] and (r[index_update] or r[index_att] or
                    (r[index_updated_at] and datetime.strptime(r[index_updated_at], '%Y-%m-%dT%H:%M:%S') > date_compare)), rows))
            else:
                updated_rows = list(filter(lambda r: r[index_id] and (r[index_update] or r[index_att]), rows))
        else:
            if is_updated_at:
                inserted_rows = list(filter(lambda r: not r[index_id] or r[index_update] or r[index_att] or
                (r[index_updated_at] and datetime.strptime(r[index_updated_at], '%Y-%m-%dT%H:%M:%S') > date_compare), rows))
            else:
                inserted_rows = list(filter(lambda r: not r[index_id] or r[index_update] or r[index_att], rows))
            updated_rows = []

    for c_index in remove_columns:
        header[c_index:c_index+1] = []
        for i in range(len(inserted_rows)):
            inserted_rows[i][c_index:c_index+1] = []
        for i in range(len(updated_rows)):
            updated_rows[i][c_index:c_index+1] = []
    
    excl_upper_row_index = [0]
    if name_att:
        index_att = header.index(name_att)
        excl_upper_row_index.append(index_att) 
    index_id = header.index('___id')
    index_update = header.index('___update')
    if is_updated_at:
        index_updated_at = header.index('lastupdate')
    header[index_att] = '___att'
    excl_upper_row_index.append(index_id)
    basedir = os.path.dirname(csv_file)

    logger.debug(f'final header {header}')

    final_header = [h for h in header] + ['created_by', 'created_at', 'updated_by', 'updated_at']

    for row in inserted_rows:
        for i in range(len(row)):
            if i not in excl_upper_row_index and row[i]:
                row[i] = row[i].strip().upper()
        if name_att:
            row[index_att] = process_attachment(attachment=row[index_att], cwd=basedir, user=user_id)
        update_at = datetime.strptime(row[index_updated_at], '%Y-%m-%dT%H:%M:%S') if is_updated_at else datetime.now()
        if update_at < date_compare:
            update_at = datetime.now()
        if not row[index_id]:
            row.append(user)
            row.append(update_at.strftime('%Y-%m-%d'))
            row.append(None)
            row.append(None)
        else:
            row.append(None)
            row.append(None)
            row.append(user)
            row.append(update_at.strftime('%Y-%m-%d'))
        row[index_update] = None
        row[index_updated_at] = None
        try:
            insert_row(conn_name, target_table, final_header, row)
        except:
            logger.debug(f'fail to insert to {target_table} value:')
            logger.debug(f'{row}')

    if len(inserted_rows) > 0:
        update_url_att(conn_name, primary_field, target_table)
    
    final_header = [h for h in header] + ['updated_by', 'updated_at']
    
    if is_sync:
        src_geo = get_geom_column(conn_name, src_table)['f_geometry_column']
        final_header[0] = src_geo
    
    for row in updated_rows:
        for i in range(len(row)):
            if i not in excl_upper_row_index and row[i]:
                row[i] = row[i].strip().upper()
        existing_att = execute_query(conn_name,
                'select "___att" from "%s" where "___id"=\'%s\'' % (src_table, row[index_id]), None, True)
        if name_att:
            row[index_att] = process_attachment(attachment=row[index_att], cwd=basedir, existing_attachment=existing_att[0]['___att'], user=user_id)
        logger.debug(f'try to update {row}')
        update_at = datetime.strptime(row[index_updated_at], '%Y-%m-%dT%H:%M:%S') if is_updated_at else datetime.now()
        if update_at < date_compare:
            update_at = datetime.now()
        row[index_update] = None
        row[index_updated_at] = None
        row.append(user)
        row.append(update_at.strftime('%Y-%m-%d'))
        try:
            update_row(conn_name, src_table, final_header, row, '___id', row[index_id], force2d=True)
        except:
            logger.debug(f'fail to update {src_table} value:')
            logger.debug(f'{row}')
    if len(updated_rows) > 0:
        update_url_att(conn_name, None, src_table)

def copy_table(conn_name, src_table, target_table):
    columns = [c['column_name'] for c in get_column_name(conn_name, target_table)]
    try:
        columns.remove('___url_att')
    except:
        pass
    is_updated_by = False

    if 'lastupdate' in columns:
        is_updated_by = True
        logger.debug('Add updateby')
        columns.remove('updated_by')
        columns.remove('lastupdate')
        columns.remove('updated_at')
        columns.remove('created_by')
        columns.remove('created_at')
    primary_field = get_primary_key(conn_name, target_table)
    target_primary_index = columns.index(primary_field)
    columns[target_primary_index:target_primary_index+1] = []
    target_geo = get_geom_column(conn_name, target_table)['f_geometry_column']
    index_att = columns.index('___att')
    index_id = columns.index('___id')
    src_geo = get_geom_column(conn_name, src_table)['f_geometry_column']
    quote_columns = [ f'"{c}"' for c in columns]

    #update
    final_quote_columns = [h for h in quote_columns] + ['"updated_by"', '"updated_at"']
    q = '''select %s, "%s" from "%s" where "___id" <> '' ''' % \
        (','.join(final_quote_columns), src_geo, src_table)
    for row in execute_query(conn_name, q, None, True, False):
        try:
            existing_att_field = execute_query(conn_name,
                    'select "___att" from "%s" where "___id"=\'%s\'' % (target_table, row[index_id]), None, True)[0]['___att']
            existing_att_field = existing_att_field.strip() if existing_att_field else ''
        except:
            logger.debug(f'current record or att not found {row[index_id]}')
            continue

        existing_att = [att for att in existing_att_field.split(';')]
        new_att = [att for att in row[index_att].split(';')]
        merge_att = list(filter(lambda r: r is not None, [ None if att in existing_att else att for att in new_att]))
        all_att = merge_att + existing_att
        if settings.GEOKINCIA['MAX_ATTACHMENT'] > 0:
            all_att = all_att[:settings.GEOKINCIA['MAX_ATTACHMENT']]
        wk_row = list(row)
        wk_row[index_att] = ';'.join(all_att)
        logger.debug(f'try to update {wk_row}')
        wk_cols = columns + ['updated_by', 'updated_at', target_geo]
        if is_updated_by:
            wk_row.append(None)
            wk_cols = wk_cols + ['lastupdate']
        
        update_row(conn_name, target_table, wk_cols, wk_row, '___id', wk_row[index_id])

    #insert
    quote_columns.remove('"___id"')
    final_quote_columns = [h for h in quote_columns] + ['"created_by"', '"created_at"']
    q = '''insert into "%s"("%s","___id", %s) select "%s",uuid_generate_v4(),%s from "%s" 
    where ("___id" = '') is not false ''' % \
        (target_table, target_geo, ','.join(final_quote_columns), src_geo, ','.join(final_quote_columns), src_table)
    
    execute_query(conn_name, q, None, False)
    update_url_att(conn_name, primary_field, target_table)
    logger.debug(f'finish merge')

    


