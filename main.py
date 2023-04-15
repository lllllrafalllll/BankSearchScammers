#!/usr/bin/python


from py_scripts.stg import Stg, Source
from py_scripts.scd2 import Scd2
from py_scripts.report import Report
import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime
import time 


#Подлючение к базе
conn_src = psycopg2.connect(database = "bank",
host = "de-edu-db.chronosavant.ru",
user = "bank_etl",
password = "bank_etl_password",
port = "5432")
conn_src.autocommit = False

cursor_src = conn_src.cursor()

# Создание подключения к PostgreSQL
conn_tgt = psycopg2.connect(database = "edu",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "de11an",
                        password = "peregrintook",
                        port =     "5432")
conn_tgt.autocommit = False

cursor_tgt = conn_tgt.cursor()

# path = f'C:/Users/rafael/Documents/SberSchool'

# os.chdir(path)

# path1 = "./project"
# path2 = "./project/archive"

path1 = "/home/de11an/mrve/project"
path2 = "/home/de11an/mrve/project/archive"

#Коммит
def comm():
    conn_tgt.commit()



#Перемещение файла
def move_archive(filename):
  os.rename(f'{path1}/{filename}', f'{path2}/{filename}.backup')

#Получение даты из файла
def dt_tgt(filename):
    text = "".join(map(lambda x: x, filter(str.isdigit, filename)))
    pattern = f'%d%m%Y'
    dt = datetime.strptime(text, pattern)
    return dt

#Чтения файла
def write_files(filename):
    files = os.listdir(path1)
    files.sort()
    for name in files:
        if name.find(filename) >= 0:
            if name.split('.')[1] == 'xlsx':
                f = pd.read_excel(f'{path1}/{name}')
                return f.reset_index(drop=True), name
            elif name.split('.')[1] == 'txt':
                f = pd.read_csv(f'{path1}/{name}', sep=';')
                return f.reset_index(drop=True), name
    return 0, '0'  


#Загрузка данных из таблица terminals в базу
def terminals():
    name = 'terminals'
    print(f'Загрузка данных из {name}')
    df, filename = write_files(name)
    if filename == '0':
        print('Файл не найден')
        return
    df['create_dt'] = dt_tgt(filename)
    table_schema = 'de11an'
    table_stg = 'mrve_stg_terminals'
    table_tgt = 'mrve_dwh_dim_terminals_hist'
    cursor = cursor_tgt
    id = 'terminal_id'

    max_meta = Stg(table_schema, table_stg, table_tgt, cursor)
    max_date = max_meta.max_meta()
    terminals = Stg(table_schema, table_stg, table_tgt, cursor, df)
    terminals.delete()
    terminals.insert_stg('create_dt', max_date)
    terminals.insert_stg_del(id)
    terminals_scd2 = Scd2()
    cursor_tgt.execute(terminals_scd2.terminals_loading())
    cursor_tgt.execute(terminals_scd2.terminals_update())
    cursor_tgt.execute(terminals_scd2.terminals_delete())
    terminals.update_meta('create_dt')
    comm()
    move_archive(filename)
    print()


#Загрузка данных из таблица blacklist в базу
def blacklist():
    name = 'passport_blacklist'
    print(f'Загрузка данных из {name}')
    df, filename = write_files(name)
    if filename == '0':
        print('Файл не найден')
        return
    table_schema = 'de11an'
    table_stg = 'mrve_stg_blacklist'
    table_tgt = 'mrve_dwh_fact_passport_blacklist'
    cursor = cursor_tgt

    max_meta = Stg(table_schema, table_stg, table_tgt, cursor)
    max_date = max_meta.max_meta()
    blacklist = Stg(table_schema, table_stg, table_tgt, cursor, df)
    blacklist.delete()
    blacklist.insert_stg('date', max_date)
    print('Загрузка в приемник "вставок" на источнике (формат Fact).')
    cursor_tgt.execute(''' insert into de11an.mrve_dwh_fact_passport_blacklist(enty_dt, passport_num)
                        select
                            date,
                            passport
                        from de11an.mrve_stg_blacklist''')
    blacklist.update_meta('date')
    comm()
    move_archive(filename)
    print()


#Загрузка данных из таблица transactions в базу
def transactions():
    name = 'transactions'
    print(f'Загрузка данных из {name}')
    df, filename = write_files(name)
    if filename == '0':
        print('Файл не найден')
        return
    df['amount'] = df['amount'].replace(regex={r',': '.'}).astype('float')
    df = df.astype({'transaction_id': 'str'})

    table_schema = 'de11an'
    table_stg = 'mrve_stg_transactions'
    table_tgt = 'mrve_dwh_fact_transactions'
    cursor = cursor_tgt

    transactions = Stg(table_schema, table_stg, table_tgt, cursor, df)
    transactions.delete()
    transactions.insert_stg()
    print('Загрузка в приемник "вставок" на источнике (формат Fact).')
    cursor_tgt.execute('''insert into de11an.mrve_dwh_fact_transactions(transaction_id, transaction_date, amount, card_num,
                            oper_type, oper_result, terminal)
                        select
                            stg.transaction_id,
                            stg.transaction_date,
                            stg.amount,
                            stg.card_num,
                            stg.oper_type,
                            stg.oper_result,
                            stg.terminal
                        from de11an.mrve_stg_transactions stg
                        left join de11an.mrve_dwh_fact_transactions tgt
                        on stg.transaction_id = tgt.transaction_id 
                        where tgt.transaction_id is null''')
    comm()
    move_archive(filename)
    print()


#Загрузка данных из базы bank из accounts в базу
def accounts():    
    table_schema = 'info'
    table_source = 'accounts'
    cursor = cursor_src
    print(f'Загрузка данных из {table_source}')

    clients = Source(table_schema, table_source, cursor)
    df = clients.select()

    table_schema = 'de11an'
    table_stg = 'mrve_stg_accounts'
    table_tgt = 'mrve_dwh_dim_accounts_hist'
    cursor = cursor_tgt

    max_meta = Stg(table_schema, table_stg, table_tgt, cursor)
    max_date = max_meta.max_meta()
    accounts = Stg(table_schema, table_stg, table_tgt, cursor, df)
    accounts.delete()
    accounts.insert_stg('create_dt', max_date)
    accounts.insert_stg_del('account')
    accounts_scd2 = Scd2()
    cursor_tgt.execute(accounts_scd2.accounts_loading())
    cursor_tgt.execute(accounts_scd2.accounts_update())
    cursor_tgt.execute(accounts_scd2.accounts_delete())
    accounts.update_meta('create_dt')
    comm()
    print()


#Загрузка данных из базы bank из clients в базу
def clients():    
    table_schema = 'info'
    table_source = 'clients'
    cursor = cursor_src
    print(f'Загрузка данных из {table_source}')

    clients = Source(table_schema, table_source, cursor)
    df = clients.select()

    table_schema = 'de11an'
    table_stg = 'mrve_stg_clients'
    table_tgt = 'mrve_dwh_dim_clients_hist'
    cursor = cursor_tgt

    max_meta = Stg(table_schema, table_stg, table_tgt, cursor)
    max_date = max_meta.max_meta()
    clients = Stg(table_schema, table_stg, table_tgt, cursor, df)
    clients.delete()
    clients.insert_stg('create_dt', max_date)
    clients.insert_stg_del('client_id')
    clients_scd2 = Scd2()
    cursor_tgt.execute(clients_scd2.clients_loading())
    cursor_tgt.execute(clients_scd2.clients_update())
    cursor_tgt.execute(clients_scd2.clients_delete())
    clients.update_meta('create_dt')
    comm()
    print()


#Загрузка данных из базы bank из cards в базу
def cards():    
    table_schema = 'info'
    table_source = 'cards'
    cursor = cursor_src
    print(f'Загрузка данных из {table_source}')
    clients = Source(table_schema, table_source, cursor)
    df = clients.select()

    table_schema = 'de11an'
    table_stg = 'mrve_stg_cards'
    table_tgt = 'mrve_dwh_dim_cards_hist'
    cursor = cursor_tgt

    max_meta = Stg(table_schema, table_stg, table_tgt, cursor)
    max_date = max_meta.max_meta()
    cards = Stg(table_schema, table_stg, table_tgt, cursor, df)
    cards.delete()
    cards.insert_stg('create_dt', max_date)
    cards.insert_stg_del('account')
    cards_scd2 = Scd2()
    cursor_tgt.execute(cards_scd2.cards_loading())
    cursor_tgt.execute(cards_scd2.cards_update())
    cursor_tgt.execute(cards_scd2.cards_delete())
    cards.update_meta('create_dt')
    comm()
    print()


#Поиск мошенников
def report():
    table_schema = 'de11an'
    table_report = 'mrve_rep_fraud'
    cursor = cursor_tgt
    report = Report()

    cursor_tgt.execute(report.report_one())
    cursor_tgt.execute(report.report_two())
    cursor_tgt.execute(report.report_three())
    cursor_tgt.execute(report.report_four())
    comm()


    

def main():
    for i in range(3):
        terminals()
        blacklist()
        transactions()
    accounts()
    clients()
    cards()  
    report()



#Запуск программы 
if __name__ == "__main__":
    print(datetime.now())
    main()


cursor_src.close()
cursor_tgt.close()
conn_src.close()
conn_tgt.close()