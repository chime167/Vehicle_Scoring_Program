#!usr/bin/env python3
import pandas as pd
import numpy as np
import re
from collections import defaultdict
import sqlite3
import json
from lxml import etree
import dicttoxml
from xml.dom.minidom import parseString
import math

print('Input file name: ')
file = input()
f, e = file.split('.')[0], file.split('.')[1]


def read_s3db(db_file, filename):
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM convoy;')
        desc = c.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row)) for row in c]
        h = [d for d in data if d['score'] > 3]
        l = [d for d in data if d['score'] < 4]
        del_key = 'score'
        high_score = [{key: val for key, val in sub.items() if key != del_key} for sub in h]
        high_score_dict = {'convoy': high_score}
        low_score = [{key: val for key, val in sub.items() if key != del_key} for sub in l]
    with open(f'{filename}.json', 'w') as json_file:
        json.dump(high_score_dict, json_file)
    if len(high_score) == 1:
        print(f'1 vehicle was saved into {filename}.json')
    else:
        print(f'{len(high_score)} vehicles were saved into {filename}.json')
    xml_df = pd.DataFrame(low_score).to_xml(root_name='convoy', index=False, row_name='vehicle', xml_declaration=False)
    with open(f'{filename}.xml', 'w') as xml_file:
        if len(low_score) == 0:
            xml_file.write('<convoy></convoy>')
        else:
            xml_file.write(xml_df)
    if len(low_score) == 1:
        print(f'1 vehicle was saved into {filename}.xml')
    else:
        print(f'{len(low_score)} vehicles were saved into {filename}.xml')
    return


def spreadsheet_parser(filename, extension):
    with sqlite3.connect(f'{filename}.s3db') as conn:
        c = conn.cursor()
    newfile = filename + '.csv'
    if extension == 's3db':
        read_s3db(file, filename)
        return
    if extension == 'xlsx':
        my_df = pd.read_excel(file, sheet_name='Vehicles', dtype=str)
        my_df.to_csv(newfile, index=None)
    else:
        my_df = pd.read_csv(file)
    count = 0
    reg = r'\D+(\d+)'
    reg2 = r'(\d+)\D+'
    df_dict = my_df.to_dict(orient='list')
    new_cells = defaultdict(list)
    if '[CHECKED]' in file:
        new_cells = df_dict
    else:
        for i, v in df_dict.items():
            for c in v:
                m1 = re.match(reg, c)
                m2 = re.match(reg2, c)
                if m1:
                    new_cells[i].append(int(m1.group(1)))
                    count += 1
                elif m2:
                    new_cells[i].append(int(m2.group(1)))
                    count += 1
                else:
                    new_cells[i].append(int(c))

    checked_mf = pd.DataFrame(new_cells)
    checked_df = pd.DataFrame(new_cells)
    checked_df['score'] = checked_df.apply(scoring_func, axis=1)
    columns = list(checked_df.to_dict().keys())
    rows = list(checked_df.to_dict().values())
    checked_name = filename + '[CHECKED].csv'
    checked_mf.to_csv(checked_name, index=None, header=None)

    filename = filename.replace('[CHECKED]', '')
    conn = sqlite3.connect(f'{filename}.s3db')
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS convoy;')
    c.execute('CREATE TABLE convoy ({} INTEGER PRIMARY KEY NOT NULL , {} INTEGER NOT NULL, {} INTEGER NOT NULL, {} INTEGER NOT NULL, {} INTEGER NOT NULL);'.format(columns[0], columns[1], columns[2], columns[3], columns[4]))
    for i, _ in enumerate(rows[0]):
        c.execute('INSERT INTO convoy VALUES (?,?,?,?,?)', (rows[0][i], rows[1][i], rows[2][i], rows[3][i], rows[4][i]))
    conn.commit()
    conn.close()

    with open(checked_name) as csv_file:
        row_count = len(csv_file.readlines())
    if extension == 'xlsx':
        if row_count == 1:
            print(f'{row_count} line was added to {filename}.csv')
        else:
            print(f'{row_count} lines were added to {filename}.csv')
    if count == 1:
        print(f'{count} cell was corrected in {checked_name}')
    if count > 1:
        print(f'{count} cells were corrected in {checked_name}')
    if row_count == 1:
        print(f'{row_count} record was inserted into {filename}.s3db')
    else:
        print(f'{row_count} records were inserted into {filename}.s3db')
    read_s3db(f'{filename}.s3db', filename)


def scoring_func(df):
    route_km = 450
    points = 0
    fuel_consumption = df['fuel_consumption']
    engine_capacity = df['engine_capacity']
    maximum_load = df['maximum_load']
    burned_fuel = (route_km * fuel_consumption / 100)
    number_of_stops = burned_fuel / engine_capacity
    if math.trunc(number_of_stops) < 1:
        points += 2
    elif math.trunc(number_of_stops) < 2:
        points += 1
    if burned_fuel <= 230:
        points += 2
    elif burned_fuel > 230:
        points += 1
    if maximum_load >= 20:
        points += 2
    return points


spreadsheet_parser(f, e)


