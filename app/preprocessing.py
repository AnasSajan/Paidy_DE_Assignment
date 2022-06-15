import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import os
import csv
import sys
import shutil
from zipfile import ZipFile


# connection details to postgres
param_dic = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": '5463'
}

# connect to database


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


conn = connect(param_dic)


zip_name = dt.datetime.now().strftime('srcdata_%m%d%Y.csv')
zip_archive = dt.datetime.now().strftime('srcdata_%m%d%Y')
# read source file
src_filename = dt.datetime.now().strftime('data/source/srcdata_%m%d%Y.csv')
print(f"Reading file...{src_filename}")
df = pd.read_csv(src_filename)

print("transforming data..")
# drop column
df = df.drop(columns=['Unnamed: 0'])

# handling NA values
df['MonthlyIncome'].fillna('', inplace=True)
df['NumberOfDependents'].fillna('', inplace=True)

# casting datatypes
df['DebtRatio'] = pd.to_numeric(df['DebtRatio'], errors='coerce')
df['NumberOfDependents'] = pd.to_numeric(
    df['NumberOfDependents'], errors='coerce')
df['MonthlyIncome'] = pd.to_numeric(df['MonthlyIncome'], errors='coerce')
df['RevolvingUtilizationOfUnsecuredLines'] = pd.to_numeric(
    df['RevolvingUtilizationOfUnsecuredLines'], errors='coerce')

# rounding values to 6 decimal places
df['DebtRatio'] = df['DebtRatio'].round(6)
df['RevolvingUtilizationOfUnsecuredLines'] = df['RevolvingUtilizationOfUnsecuredLines'].round(
    6)


# days late column business logic
df.loc[(df['NumberOfTime60-89DaysPastDueNotWorse'] + df['NumberOfTime30-59DaysPastDueNotWorse']
        ).gt(24), ['NumberOfTime60-89DaysPastDueNotWorse', 'NumberOfTime30-59DaysPastDueNotWorse']] = ''
df.rename(columns={'SeriousDlqin2yrs': 'serious_dlq_2', 'RevolvingUtilizationOfUnsecuredLines': 'revolving_utilization', 'DebtRatio': 'debt_ratio', 'NumberOfTime30-59DaysPastDueNotWorse': 'past_due_days_30-59', 'NumberOfTime60-89DaysPastDueNotWorse': 'past_due_days_60-89',
                   'MonthlyIncome': 'monthly_income', 'NumberOfOpenCreditLinesAndLoans': 'open_credit_lines_and_loans', 'NumberOfTimes90DaysLate': '90_days_late', 'NumberRealEstateLoansOrLines': 'real_estate_loans_or_lines', 'NumberOfDependents': 'number_of_dependents'}, inplace=True)

# metadata column
df['created_ts'] = dt.datetime.now().strftime('%m-%d-%Y %H:%M:%S')

# loading data into csv
target_folder = dt.datetime.now().strftime('data/target/%m%Y')

target_folder_archive = dt.datetime.now().strftime('data/archive/%m%Y')

target_folder_failed = dt.datetime.now().strftime('data/failed/%m%Y')

pre_load_folder = dt.datetime.now().strftime('data/preload/%m%Y')

tgt_filename = dt.datetime.now().strftime(
    f'{target_folder}/tgtdata_%m%d%Y.csv')

tgt_archived_filename = dt.datetime.now().strftime(
    f'{target_folder_archive}/tgtdata_%m%d%Y.csv')

pre_load_filename = dt.datetime.now().strftime(
    f'{pre_load_folder}/preload_data_%m%d%Y.csv')

failed_filename = dt.datetime.now().strftime(
    f'{target_folder_failed}/failed_%m%d%Y.csv')

if not os.path.exists(pre_load_folder):
    os.mkdir(pre_load_folder)
df.to_csv(pre_load_filename, index=False)

# load data into postgres
table = 'P_LOAN_APPLICATION'
sql = """COPY {table}(SERIOUS_DLQ_2, REVOLVING_UTILIZATION, AGE, PAST_DUE_DAYS_30_59,DEBT_RATIO,MONTHLY_INCOME,OPEN_CREDIT_LINES_AND_LOANS,PLUS_DAYS_LATE_90,REAL_ESTATE_LOANS_OR_LINES,PAST_DUE_DAYS_60_89,NUMBER_OF_DEPENDENTS,CREATED_TS) FROM STDIN WITH (DELIMITER '{sep}', NULL '', FORMAT CSV, HEADER True);""".format(table=table, sep=',')
cur = conn.cursor()
try:
    with open(pre_load_filename, 'r') as f:
        cur.copy_expert(sql=sql, file=f)
        conn.commit()
        print("loading data...")
        if not os.path.exists(target_folder):
            os.mkdir(target_folder)
        df.to_csv(tgt_filename, index=False)
        print(f'loaded data in file {tgt_filename}')
        print(f'Data Ingested in table {table} successfully')
        # zip source file on success and send into archives
        print(f"zipping source file {src_filename} ....")
        print("Moving to Achrives...")
        if not os.path.exists(target_folder_archive):
            os.mkdir(target_folder_archive)
        shutil.make_archive(src_filename, 'zip', target_folder_archive)
        shutil.move(f"{src_filename}.zip",
                    f"{target_folder_archive}/{zip_archive}.zip")
        print(f"Moved file to {target_folder_archive} as {zip_archive}.zip")
        print("Removing source file..")
        os.remove(src_filename)
        print("Removed..")
    print("Removing preload file..")
    os.remove(pre_load_filename)
    print("Successful!!")
except (Exception, psycopg2.DatabaseError) as error:
    if not os.path.exists(target_folder_failed):
        os.mkdir(target_folder_failed)
    df.to_csv(failed_filename, index=False)
    print("Error: %s" % error)
    print(
        f'Failed to Load!..Please check {failed_filename} for more details..')
    print("Removing preload file..")
    os.remove(pre_load_filename)
    print("Removed..")
    cur.execute("ROLLBACK")
    cur.close()
