from os import sync
from django.db import connections
from django.conf import settings
from datetime import datetime

import re
import csv
import os
import shutil
import uuid
import logging

ATTACHMENT = ('attachment', 'Attachment', 'attachments', 'Attachments')
PHOTO_EXT = ['.png', '.jpg', '.jpeg', '.svg']
VIDEO_EXT = ['.mp4', '.avi']

logger = logging.getLogger(__name__)

def process_attachment(attachment, cwd='.', existing_attachment=''):
    new_attachments = []
    attachment_dir = settings.GEOKINCIA['ATTACHMENT_DIR']
    existing_attachments = existing_attachment.split(';')
    existing_attachments_files = [att.split('#')[0] for att in existing_attachments]
    for att in attachment.split(','):
        origin = os.path.join(cwd, att.strip())
        base, f = os.path.split(origin)
        target = os.path.join(attachment_dir, f)
        if not f in existing_attachments_files:
            f_prop = f
            _,ext = os.path.splitext(origin)
            if ext in PHOTO_EXT:
                f_prop += '#photo'
            elif ext in VIDEO_EXT:
                f_prop += '#video'
            else:
                continue
            stat = os.stat(origin)
            f_prop += f'#{datetime.fromtimestamp(stat.st_atime).strftime("%a %d-%m-%Y")}'
            if not os.path.exists(target):
                shutil.copy2(origin, attachment_dir)
            new_attachments.append(f_prop)
    return ';'.join(new_attachments + existing_attachments)


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def execute_query(conn_name, query, params, is_return, is_dict=True):
    '''Execute query'''
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

def get_column_name(conn_name, table_name):
    q = '''select column_name, data_type from information_schema.columns where table_schema = 'public'
        and table_name= %s and data_type <> 'USER-DEFINED'''
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
        WHERE  i.indrelid = '%s'::regclass
        AND    i.indisprimary;''' % table_name
    pr = execute_query(conn_name, q, None, True)
    return pr[0]['attname']


def insert_row(conn_name, table_name, colums, values, add_multi=None, target_geo='', geo_value=''):
    colums_txt = ','.join([ '"%s"' % c for c in colums])
    values_txt = ','.join(['null' if v is None else "'%s'" % v for v in values])
    if add_multi:
        q = 'insert into %s("%s", %s) values(st_multi(\'%s\'), %s)' % (table_name, target_geo, colums_txt, geo_value, values_txt)
    else:
        q = 'insert into(%s) values(%s)' % (table_name, colums_txt, values_txt)
    execute_query(conn_name,q, None, False)

def update_row(conn_name, table_name, columns, values, col_id, col_id_value, add_multi=None, target_geo='', geo_value=''):
    update_values = []
    for i in range(len(columns)):
        value = "null" if values[i] is None else "%s='%s'" % (columns[i], values[i])
        update_values.append(value)

    q = "update %s set %s where %s='%s' " % (table_name, ','.join(update_values), col_id, col_id_value)
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
    def _to_multi(row):
        coord_pair = re.findall(r'(\([0-9.,() \-Nan]*\))', row[index_geom])[0]
        geo_type = re.findall(r'(LINE|POINT|POLYGON)', row[index_geom])[0]
    
        row[index_geom] = 'MULTI' + geo_type + ' (' + coord_pair + ')'
        return row
    def _to_single(row):
        coord_pair = re.findall(r'(\([0-9\.\ () \-Nan]*\))', row[index_geom])[0]
        geo_type = re.findall(r'(LINE|POINT|POLYGON)', row[index_geom])[0]
        
        row[index_geom] = geo_type + ' ' + coord_pair[1:len(coord_pair) -1]
        return row

    if target_geom[1] != src_geom[1]:
        raise Exception
    if target_geom[0]+target_geom[1] == src_geom[0]+src_geom[1]:
        return rows
    
    if target_geom[0] == 'MULTI' and src_geom[0] != 'MULTI':
        return list(map(_to_multi, rows))

    elif target_geom[0] != 'MULTI' and src_geom[0] == 'MULTI':
        return list(map(_to_single, rows))

def load_from_csv(conn_name, csv_file, target_table, is_sync, auto_fill=False):
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
        row[i] = [r if len(r) > 0 else None for r in rows[i]]

    index_id = header.index('___id')
    index_geom = 0

    index_att = None
    name_att = ''
    for i in range(len(header)):
        if header[i] in ATTACHMENT:
            index_att = i
            name_att = header[i]
            break

    target_columns = [c['column_name'] for c in get_column_name(conn_name, target_table)]
    target_columns.append(name_att)
    remove_columns = ['___att']
    remove_columns.append(get_primary_key(conn_name, target_table))

    for column in header:
        if not column in target_columns:
            remove_columns.append(header.index(column))
    remove_columns.sort()
    remove_columns.reverse()
    
    logger.debug(f'remove column : {remove_columns}')

    for c_index in remove_columns:
        header[c_index:c_index+1] = []
        for i in range(len(rows)):
            rows[i][c_index:c_index+1] = []
    header[index_att] = '___att'

    if auto_fill:
        try:
            index_misc = header.index('___misc')
            row = rows[0]
            for i in range(len(rows)):
                if str(rows[i][index_misc]).strip().lower() == 'auto':
                    for j in range(len(row)):
                        if not rows[i][j] and not j in [0, index_att, index_id]:
                            rows[i][j] = row[j]
                row = rows[i][j]
        except:
            logger.debug('fail to auto fill check internal_misc)')
    try:
        target_geo = get_geom_column(conn_name, target_table)
        target_geo_type = re.findall(r'(MULTI)*(\w+)', target_geo['type'], re.IGNORECASE)
        src_geo_type = re.findall(r'(MULTI)*(\w+)', rows[0][0], re.IGNORECASE)
        logger.debug(f'before row geo update: {row[0]}')
        rows = update_csv_geom(target_geo_type[0], src_geo_type[0], rows, index_geom)
        logger.debug(f'after row geo update: {row[0]}')
    except:
        raise Exception

    if is_sync:
        inserted_rows = list(filter(lambda r: not r[index_id], rows))
        for row in inserted_rows:
            row[index_id] = str(uuid.uuid4())
        updated_rows = list(filter(lambda r: r[index_id], rows))
    else:
        inserted_rows = rows

    basedir = os.path.dirname(csv_file)
    logger.debug(f'final header {header}')
    if inserted_rows:
        for row in inserted_rows:
            row[index_att] = process_attachment(row[index_att], basedir)
            logger.debug(f'try to insert {row}')
            insert_row(conn_name, target_table, header, row)
    if updated_rows:
        for row in updated_rows:
            existing_att = execute_query(conn_name,
                'select "___att" from "%s" where "___id"=\'%s\'' % (target_table, row[index_id]), None, True)
            row[index_att] = process_attachment(row[index_att], basedir, existing_att[0]['___att'])
            logger.debug(f'try to update {row}')
            update_row(conn_name, target_table, header, row, '___id', row[index_id])




