#!/usr/bin/env python3

import pandas as pd
import pysqlite3
import json
from math import trunc
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('filename', help='Enter the filename or path to file')
args = parser.parse_args()

file = args.filename
f, e = file.split('.')[0], file.split('.')[1]


def read_s3db(db_file, filename):
    with pysqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM convoy;')
        desc = c.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row)) for row in c]
        # separating low and high scores into separate groups
        h = [d for d in data if d['score'] > 3]
        l = [d for d in data if d['score'] < 4]
        del_key = 'score'
        high_score = [{key: val for key, val in sub.items() if key != del_key} for sub in h]
        high_score_dict = {'convoy': high_score}
        low_score = [{key: val for key, val in sub.items() if key != del_key} for sub in l]
    # the high scores go to json file    
    with open(f'{filename}.json', 'w') as json_file:
        json.dump(high_score_dict, json_file)
    if len(high_score) == 1:
        print(f'1 vehicle was saved into {filename}.json')
    else:
        print(f'{len(high_score)} vehicles were saved into {filename}.json')
    xml_df = pd.DataFrame(low_score).to_xml(root_name='convoy', index=False, row_name='vehicle', xml_declaration=False)
    # low scores go to xml files
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
    try:
        with pysqlite3.connect(f'{filename}.s3db') as conn:
            c = conn.cursor()
    except Exception as e:
        print(f'There was an error opening the {extension} file. Check the path and make sure it exists.', e)
        exit()
    newfile = filename + '.csv'
    # if it's already a s3db file then it has been checked and scored
    if extension == 's3db':
        try:
            read_s3db(file, filename)
            return
        except Exception as e:
            print(f'There was an error opening the {extension} file. Check the path and make sure it exists.', e)
            exit()
    if extension == 'xlsx':
        try:
            my_df = pd.read_excel(file, sheet_name='Vehicles', dtype=str)
            my_df.to_csv(newfile, index=None)
        except Exception as e:
            print(f'There was an error opening the {extension} file. Check the path and make sure it exists.', e)
            exit()
    else:
        try:
            my_df = pd.read_csv(file)
            count = my_df.apply(lambda x: x.str.contains(r'\D').sum(), axis=1)
            count = sum([*filter(lambda x: x, count)])
        except Exception as e:
            print(f'There was an error opening the {extension} file. Check the path and make sure it exists.', e)
            exit()
    my_df = my_df.replace(to_replace='\D', value='', regex=True)
    my_df = my_df.astype('int64')


    checked_mf = my_df
    my_df['score'] = scoring_func(my_df)
    columns = list(my_df.to_dict().keys())
    rows = list(my_df.to_dict().values())
    checked_name = filename + '[CHECKED].csv'
    checked_mf.to_csv(checked_name, index=None, header=None)

    filename = filename.replace('[CHECKED]', '')
    conn = pysqlite3.connect(f'{filename}.s3db')
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



# scoring function based on client's specifications

def scoring_func(df):
    route_km = 450
    points = 0
    fuel_consumption = df['fuel_consumption']
    engine_capacity = df['engine_capacity']
    maximum_load = df['maximum_load']
    burned_fuel = (route_km * fuel_consumption / 100)
    number_of_stops = (burned_fuel / engine_capacity).apply(trunc)
    points += (2 * (number_of_stops < 1).astype(int)) + (1 * ((1 <= number_of_stops) & (2 > number_of_stops)).astype(int))
    points += (2 * (burned_fuel <= 230).astype(int)) + (1 * (burned_fuel > 230).astype(int))
    points += (2 * (maximum_load >= 20).astype(int))
    return points

if __name__ == '__main__': spreadsheet_parser(f, e)