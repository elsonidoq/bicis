from datetime import datetime, timedelta
import pandas as pd
from itertools import chain

from tqdm import tqdm
from glob import glob
import os
import csv
import re


def load_data_frame(n=None):
    fnames = glob(os.path.join(here, '../data/bicicletas-publicas/recorridos-realizados-*csv'))

    iterators = []
    for fname in fnames:
        iterators.append(iter_fname(fname, n=n))

    return pd.DataFrame(list(tqdm(chain(*iterators))))

STRICT_MAPPING = True
dp = re.compile('\d+')
here = os.path.dirname(__file__)

fields_v1 = dict(
    origenfecha='rent_date',
    nombreorigen='rent_station',
    destinofecha='return_date',
    nombredestino='return_station',
    tiempouso='duration'
)

fields_v2 = dict(
    origenfecha='rent_date',
    origennombre='rent_station',
    destinofecha='return_date',
    destinonombre='return_station',
    tiempouso='duration'
)

fields_v3 = dict(
    origen_fecha='rent_date',
    destino_fecha='return_date',
    nombre_origen='rent_station',
    destino_estacion='return_station',
)

fields_v4 = dict(
    fecha_hora_retiro='rent_date',
    tiempo_uso='duration',
    nombre_origen='rent_station',
    nombre_destino='return_station',
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


def format_reader(csv_reader, row_mapping=None):
    row_mapping = row_mapping or {}

    for li_no, doc in enumerate(csv_reader):
        doc = {k.lower().strip(): v for k, v in doc.iteritems()}

        new_doc = {}
        for k, v in doc.iteritems():
            if k not in row_mapping and STRICT_MAPPING: continue
            if v.isdigit(): v = int(v)
            new_doc[row_mapping.get(k, k)] = v

        for field in 'rent_date return_date'.split():
            if field not in new_doc: continue
            errors = 0

            # sometimes there's a different format for each field
            for date_format in ['%d/%m/%Y %H:%M','%Y-%m-%d %H:%M:%S.%f']:
                try:
                    new_doc[field] = datetime.strptime(new_doc[field], date_format)
                    break
                except ValueError, e:
                    errors += 1

            if errors == 2:
                break

        if errors == 2:
            print "Could not parse doc"
            continue

        if 'duration' in new_doc:
            new_doc['duration'] = parse_duration(new_doc['duration'])

        if 'return_date' not in new_doc:
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

    print "Finished iterating {}".format(os.path.basename(fname))


