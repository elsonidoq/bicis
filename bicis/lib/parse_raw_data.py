from datetime import datetime, timedelta
import pandas as pd
from itertools import chain

from tqdm import tqdm
from glob import glob
import os
import csv
import re


STRICT_MAPPING = True
dp = re.compile('\d+')
here = os.path.dirname(__file__)

fields_v1 = dict(
    origenfecha='rent_date',
    nombreorigen='src_station',
    destinofecha='return_date',
    nombredestino='dst_station',
    tiempouso='duration'
)

fields_v2 = dict(
    origenfecha='rent_date',
    origennombre='src_station',
    destinofecha='return_date',
    destinonombre='dst_station',
    tiempouso='duration'
)

fields_v3 = dict(
    origen_fecha='rent_date',
    destino_fecha='return_date',
    nombre_origen='src_station',
    destino_estacion='dst_station',
)

fields_v4 = dict(
    fecha_hora_retiro='rent_date',
    tiempo_uso='duration',
    nombre_origen='src_station',
    nombre_destino='dst_station',
)

row_mappings = {
    2010: fields_v2,
    2011: fields_v1,
    2012: fields_v2,
    2013: fields_v3,
    2014: fields_v3,
    2015: fields_v4,
    2016: fields_v4,
    2017: fields_v4,
}


def parse_duration(d):
    if isinstance(d, int):
        return timedelta(seconds=60*int(d))
    elif d:
        parts = [int(dp.search(e).group(0)) for e in d.split()][:3]
        if len(parts) < 3: return

        h, m, s = parts
        s += m*60
        return timedelta(hours=h, seconds=s)


def parse_date(date_string):
    date_formats = ['%d/%m/%Y %H:%M', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']

    for date_format in date_formats:
        try:
            return datetime.strptime(date_string.strip(), date_format)
        except ValueError:
            pass

    raise ValueError('Could not parse date')


def format_reader(csv_reader, row_mapping=None):
    row_mapping = row_mapping or {}


    for li_no, doc in enumerate(csv_reader):
        doc = {k.lower().strip(): v for k, v in doc.iteritems()}

        new_doc = {}
        for k, v in doc.iteritems():
            if k not in row_mapping and STRICT_MAPPING: continue
            v = v.strip()
            if v.isdigit(): v = int(v)
            new_doc[row_mapping.get(k, k)] = v

        field_with_date_error = None
        for field in 'rent_date return_date'.split():
            if field not in new_doc: continue

            # sometimes there's a different format for each field
            try:
                new_doc[field] = parse_date(new_doc[field])
            except ValueError:
                field_with_date_error = field, new_doc[field]
                break

        if field_with_date_error:
            print "Could not parse date field {} with value {}".format(*field_with_date_error)
            continue

        if 'duration' in new_doc:
            new_doc['duration'] = parse_duration(new_doc['duration'])

        if 'return_date' not in new_doc and new_doc.get('duration'):
            new_doc['return_date'] = new_doc['rent_date'] + new_doc['duration']

        yield new_doc


def limit(iterator, n):
    for i, e in enumerate(iterator):
        if i == n: break
        yield e


def iter_fname(fname, n=None):
    period = int(fname.split('-')[-1].split('.')[0])
    with open(fname) as f:
        reader = csv.DictReader(f, delimiter=';')
        iterator = format_reader(
            reader,
            row_mapping=row_mappings[period],
        )

        if n is not None:
            iterator = limit(iterator, n)

        for doc in iterator:
            yield doc

