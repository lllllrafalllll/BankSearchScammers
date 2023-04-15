import pandas as pd
import psycopg2

#Класс для получении данных из базы bank
class Source:

    def __init__(self, table_schema, table_source, cursor) -> None:        
        self.table_schema = table_schema
        self.table_source = table_source
        self.cursor = cursor

    def select(self):
        self.cursor.execute(f"""select * from {self.table_schema}.{self.table_source}""")
        records = self.cursor.fetchall()
        names = [ x[0] for x in self.cursor.description ]
        table = pd.DataFrame( records, columns = names )
        return table




class Stg:

    def __init__(self, table_schema, table_stg, table_tgt, cursor, df=None) -> None:
        self.table_schema = table_schema
        self.table_stg = table_stg
        self.table_tgt = table_tgt
        self.df = df
        self.cursor = cursor

    def select(self):
        self.cursor.execute(f"""select * from {self.table_schema}.{self.table_stg}""")
        records = self.cursor.fetchall()
        names = [ x[0] for x in self.cursor.description ]
        table = pd.DataFrame( records, columns = names )
        return table

#Получение наименование колонок из таблице в формате для загрузки в sql
    @staticmethod
    def query(data):
        cols = list(data.columns)
        query = ' ,'.join(cols)
        return query

#Получение кол-во колонок из таблице в формате для загрузки в sql
    @staticmethod
    def values(data):
        cols = list(data.columns)
        values = ' ,'.join(['%s' for _ in range(len(cols))])
        return values


    #Получаем максимальную дату из meta
    def max_meta(self):
        self.cursor.execute( f""" select max_update_dt from {self.table_schema}.mrve_meta
                        where schema_name='{self.table_schema}' and table_name = '{self.table_stg}' """)
        last_terminals_date = self.cursor.fetchone()[0]
        print('Получаем максимальную дату из meta')
        return last_terminals_date



    #1. Очистка стейджинговых таблиц
    def delete(self):
        self.cursor.execute(f"""delete from  {self.table_schema}.{self.table_stg}""")
        self.cursor.execute(f"""delete from  {self.table_schema}.mrve_stg_del""")
        print('Очистка стейджинговых таблиц')

    
    #-- 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
    def insert_stg(self, col_name_date=None, max_date=None):
        self.col_name_date = col_name_date
        self.max_date = max_date
        df_ = self.df
        if max_date is not None:
            df_ = self.df[self.df[col_name_date] > max_date]
        self.cursor.executemany(f""" INSERT INTO {self.table_schema}.{self.table_stg}(
                                {self.query(self.df)}
                            ) VALUES( {self.values(self.df)} ) """, df_.values.tolist() )
        print('Захват данных из источника (измененных с момента последней загрузки) в стейджинг')

    
    #-- 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
    def insert_stg_del(self, id):
        self.id = id
        df_id = pd.DataFrame(self.df[self.id].drop_duplicates())
        self.cursor.executemany( f""" INSERT INTO {self.table_schema}.mrve_stg_del(
                                id
                            ) VALUES( %s ) """, df_id.values.tolist() )

        print('Захват в стейджинг ключей из источника полным срезом для вычисления удалений.')
                        

    #-- 7. Обновление метаданных.
    def update_meta(self, col_name_date):
        self.col_name_date = col_name_date
        self.cursor.execute(f'''update  {self.table_schema}.mrve_meta
                            set max_update_dt = coalesce(
                            ( select max( {col_name_date} ) from {self.table_schema}.{self.table_stg}),
                            ( select max_update_dt from {self.table_schema}.mrve_meta
                            where schema_name='{self.table_schema}' and table_name='{self.table_stg}' ))
                            where schema_name='{self.table_schema}' and table_name = '{self.table_stg}' ''')
        
        print('Обновление метаданных')










