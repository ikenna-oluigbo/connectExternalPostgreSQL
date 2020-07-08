'''
Created on June 28, 2020
@author: ikenna oluigbo
@email: ikenna.oluigbo@gmail.com
'''

import requests
import pandas as pd
import csv
import psycopg2
import psycopg2.extras
import argparse
import time
pd.set_option('display.expand_frame_repr', False)   #Show all Columns
pd.options.display.max_rows = None 


def parse_args():
    parser = argparse.ArgumentParser(description="Run GIMACS")
    parser.add_argument('--input', default='GIMACS.csv', help='Input csv file path')
    parser.add_argument('--port', default=5432, help='Port Number')
    parser.add_argument('--localusername', default='postgres', help='Postgre Username')
    parser.add_argument('--localpassword', default='Enter your postgresql password', help='Postgre Password')
    parser.add_argument('--localdatabase', default='Enter your postgresql DB name', help='Postgre database Name')
    parser.add_argument('--extusername', default='Enter your external DB username', help='EXT_DB Username')
    parser.add_argument('--extpassword', default='Enter your external DB password', help='EXT_DB Password')
    parser.add_argument('--extdatabase', default='Enter your external DB name', help='EXT_DB Name')
    parser.add_argument('--exthost', default='Enter your external DB hostname', help='EXT_DB HOST Name')
    parser.add_argument('--csvdata', default='Enter link to the CSV resource', help='Online CSV Resource')
    return parser.parse_args()

args = parse_args()


#CONNECTING TO LOCAL POSTGRESQL DATABASE**********
class locDB:
    
    def collate_updated_data(self):
        print('Initiating Connection to api to retrieve Updates... \n'); time.sleep(5)
        CSV_URL = args.csvdata
        print('Collating Data from api... \n')
        with requests.Session() as sess:
            response = sess.get(CSV_URL)
            decoded_content_response = response.content.decode('utf-8')
            file = csv.reader(decoded_content_response.splitlines(), delimiter=',')
            formatted_file = list(file)
        
        file = csv.writer(open("GIMACS.csv", "w+", newline=''))
        for i in formatted_file:
            file.writerow(i)
        print('Collation Completed... \n')
            
    def read_file(self):
        data = pd.read_csv(args.input)
        table_cols = [i for i in data.columns]
        return data, table_cols
    
        
    def connect_postgresql(self):
        '''Database Instance OWIDCOVID already created in PostgreSQL server
           On Local Machine, default host = localhost and default port = 5432'''
        
        connect_db = psycopg2.connect(host="localhost", database=args.localdatabase, user=args.localusername, password=args.localpassword)
        return connect_db
    
    def create_tables(self):
        DB_TABLE = (
                    """
                    CREATE TABLE owid_covid (
                        iso_code varchar,
                        continent varchar, 
                        location varchar,
                        date date,
                        total_cases varchar,
                        new_cases varchar,
                        total_deaths varchar,
                        new_deaths varchar,
                        total_cases_per_million varchar,
                        new_cases_per_million varchar,
                        total_deaths_per_million varchar,
                        new_deaths_per_million varchar,
                        total_tests varchar,
                        new_tests varchar,
                        total_tests_per_thousand varchar,
                        new_tests_per_thousand varchar,
                        new_tests_smoothed varchar,
                        new_tests_smoothed_per_thousand varchar,
                        tests_units varchar,
                        stringency_index varchar,
                        population varchar,
                        population_density varchar,
                        median_age varchar,
                        aged_65_older varchar,
                        aged_70_older varchar,
                        gdp_per_capita varchar,
                        extreme_poverty varchar,
                        cvd_death_rate varchar,
                        diabetes_prevalence varchar,
                        female_smokers varchar,
                        male_smokers varchar,
                        handwashing_facilities varchar,
                        hospital_beds_per_thousand varchar,
                        life_expectancy varchar
                    )"""
                )
        
        connect_db = local.connect_postgresql()
        curs = connect_db.cursor()
        curs.execute(DB_TABLE)
        curs.close()
        connect_db.commit()
        connect_db.close()
    
    
    def get_columns(self):
        connect_db = local.connect_postgresql()
        curs = connect_db.cursor()
        curs.execute("select column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'owid_covid'")
        cols = curs.fetchall()
        curs.close()
        connect_db.close()
        db_cols = [col[0] for col in cols]
        strg = '%s,'*len(db_cols)
        strg = strg.rstrip(',')
        return db_cols, strg
    
    
    def insert_data(self, owid_covid_data, table_cols):
        print("Merging New Updates with Local Database... \n")
        connect_db = local.connect_postgresql()
        curs = connect_db.cursor()
        
        db_cols, strg = local.get_columns()
        if table_cols != db_cols:
            db_cols = table_cols
        DB_data = "INSERT INTO owid_covid ({}) VALUES ({})".format(', '.join(db_cols), strg)
        
        curs.executemany(DB_data, owid_covid_data)
        connect_db.commit()
        curs.close()
        connect_db.close()
        print("New Updates Merged with Local Database Successfully!! \n")
    
    
    def query_owid_data(self):
        connect_db = local.connect_postgresql()
        curs = connect_db.cursor()
        curs.execute("SELECT * FROM owid_covid")
        row = curs.fetchall()
        curs.close()
        connect_db.close()
        return row

    
    def remove_old_data(self):
        connect_db = local.connect_postgresql()
        curs = connect_db.cursor()
        curs.execute("DELETE FROM owid_covid")
        connect_db.commit()
        curs.close()
        connect_db.close()
        print('Local Database Table Edited Successfully!! \n')


#CONNECTING TO EXTERNAL DATABASE ********************
class ExtDB:
    
    def ext_server(self):
        ex_db_connect = psycopg2.connect(host=args.exthost, \
                                      database=args.extdatabase, user=args.extusername, password=args.extpassword, \
                                      port=args.port)
        return ex_db_connect
    
    
    def create_ex_tables(self):
        EX_DB_TABLE = (
                    """
                    CREATE TABLE world_in_data_covid19 (
                        iso_code varchar,
                        continent varchar, 
                        location varchar,
                        date date,
                        total_cases varchar,
                        new_cases varchar,
                        total_deaths varchar,
                        new_deaths varchar,
                        total_cases_per_million varchar,
                        new_cases_per_million varchar,
                        total_deaths_per_million varchar,
                        new_deaths_per_million varchar,
                        total_tests varchar,
                        new_tests varchar,
                        total_tests_per_thousand varchar,
                        new_tests_per_thousand varchar,
                        new_tests_smoothed varchar,
                        new_tests_smoothed_per_thousand varchar,
                        tests_units varchar,
                        stringency_index varchar,
                        population varchar,
                        population_density varchar,
                        median_age varchar,
                        aged_65_older varchar,
                        aged_70_older varchar,
                        gdp_per_capita varchar,
                        extreme_poverty varchar,
                        cvd_death_rate varchar,
                        diabetes_prevalence varchar,
                        female_smokers varchar,
                        male_smokers varchar,
                        handwashing_facilities varchar,
                        hospital_beds_per_thousand varchar,
                        life_expectancy varchar
                    )"""
                )
        
        ex_db_connect = owid.ext_server()
        curs = ex_db_connect.cursor()
        curs.execute(EX_DB_TABLE)
        curs.close()
        ex_db_connect.commit()
        ex_db_connect.close()
        
        
    def insert_data_external(self):
        print('FETCHING UPDATES FROM SOURCE ... \n')
        ex_db_connect = owid.ext_server()
        curs = ex_db_connect.cursor()
        DB_data = "INSERT INTO world_in_data_covid19 (iso_code, continent, location, date, total_cases, new_cases,\
                    total_deaths, new_deaths, total_cases_per_million, new_cases_per_million, total_deaths_per_million,\
                   new_deaths_per_million, total_tests, new_tests, total_tests_per_thousand, new_tests_per_thousand,\
                   new_tests_smoothed, new_tests_smoothed_per_thousand, tests_units, stringency_index, population,\
                   population_density, median_age, aged_65_older, aged_70_older, gdp_per_capita, extreme_poverty,\
                   cvd_death_rate, diabetes_prevalence, female_smokers, male_smokers, handwashing_facilities,\
                   hospital_beds_per_thousand, life_expectancy) \
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" 
        
        print("INSERTION STARTED INTO EXTERNAL DATABASE ... \n")
        local_owid_data = locDB.query_owid_data(self)
        psycopg2.extras.execute_batch(curs, DB_data, local_owid_data)
        ex_db_connect.commit()
        curs.close()
        ex_db_connect.close()
        print("INSERTION COMPLETED ...")
    
    def remove_old_ex_data(self):
        ex_db_connect = owid.ext_server()
        curs = ex_db_connect.cursor()
        print("Removing Redundant Data... \n")
        curs.execute("DELETE FROM world_in_data_covid19")
        ex_db_connect.commit()
        curs.close()
        ex_db_connect.close()
        print("Removal of Redundant Data Completed \n")
    

if __name__ == '__main__':
    print("WORKING ON LOCAL DATABASE")
    local = locDB()
    local.collate_updated_data()
    local.remove_old_data()
    d, table_cols = local.read_file()
    raw_d = d.copy()
    raw_d = raw_d.fillna(value='Null')
    owid_covid_data = [tuple(raw_d.iloc[i].values) for i in range(d.shape[0])]
    local.insert_data(owid_covid_data, table_cols)
    
    print("WORKING ON EXTERNAL DATABASE")
    owid = ExtDB()
    #owid.create_ex_tables()
    owid.remove_old_ex_data()
    owid.insert_data_external()









