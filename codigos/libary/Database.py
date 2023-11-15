#Essa Classe se conecta no banco de dados e realiza uma query de acordo com a query informada

from interface.DatabaseInterface import DatabaseInterface
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import psycopg2


class Database:

    def __init__(self, schema:str, table:str, ambiente:int):
        self.schema = schema
        self.table = table
        self.dotenv_path = False
        self.ambiente = ambiente
    
    def create_connect(self):
        try:
            self.dotenv_path = load_dotenv(dotenv_path=r'./.env', override=True)
            try:
                AMBIENTE = {0:"PRODUCAO", 1:"HOMOLOGACAO"}
                engine = create_engine(os.environ.get(f"SQLALCHEMY_{AMBIENTE[self.ambiente]}"))
                self.conn = engine.connect()
            except Exception as e:
                    print(f'Falha ao estabeler conexão com banco de dados : {e}')
        except Exception as e:
            pass
        finally:
            if self.dotenv_path == False:
                print(f"Falha ao importar as variaveis de ambiente.")

    def close_connect(self):
        try:
            if self.conn is not None:
                if self.conn.closed == False:
                    self.conn.close()
        except Exception as e:
            print(f'Erro ao fechar conexão: {e}')


    def select_table(self, query=None):
        try:
            self.create_connect()
            if query is None:
                return pd.read_sql(text(f'select * from {self.schema}.{self.table}'), self.conn)
            else:
                return pd.read_sql(text(query), self.conn)
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: {e}')
            return None
        finally:
            self.close_connect()

    def to_table(self, df, schema=None, table=None):
        try:
            self.create_connect()
            if query is None:
                self.truncate_table()
                df.to_sql(self.table, schema=self.schema, conn= self.conn, if_exists='append')
            else:
                self.truncate_table(table=table, schema=schema)
                df.to_sql(table=table, schema=schema, conn= self.conn, if_exists='append')
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: {e}')
            return None
        finally:
            self.close_connect()

    def refresh_table(self, table=None, schema=None):
        AMBIENTE = {0:"PRODUCAO", 1:"HOMOLOGACAO"}
        DATABASE_HOMOLOGACAO = os.environ.get(f"DATABASE_{AMBIENTE[self.ambiente]}")
        USER_HOMOLOGACAO = os.environ.get(f"USER_{AMBIENTE[self.ambiente]}")
        PASSWORD_HOMOLOGACAO = os.environ.get(f"PASSWORD_{AMBIENTE[self.ambiente]}")
        HOST_HOMOLOGACAO = os.environ.get(f"HOST_{AMBIENTE[self.ambiente]}")
        PORT_HOMOLOGACAO = os.environ.get(f"PORT_{AMBIENTE[self.ambiente]}")
        try:
            self.create_connect()
            if query is None:
                try:
                    conn =  psycopg2.connect(database=DATABASE_HOMOLOGACAO, \
                                            user=USER_HOMOLOGACAO, \
                                            password=PASSWORD_HOMOLOGACAO, \
                                            host=HOST_HOMOLOGACAO, \
                                            port=PORT_HOMOLOGACAO)
                    conn.autocommit = True
                    cursor = conn.cursor()
                    print(cursor.execute(f"refresh materialized view {self.schema}.{self.table}"))
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    print('erro')
            else:
                try:
                    conn =  psycopg2.connect(database=DATABASE_HOMOLOGACAO, \
                                            user=USER_HOMOLOGACAO, \
                                            password=PASSWORD_HOMOLOGACAO, \
                                            host=HOST_HOMOLOGACAO, \
                                            port=PORT_HOMOLOGACAO)
                    conn.autocommit = True
                    cursor = conn.cursor()
                    cursor.execute(table=table, schema=schema) 
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: {e}')
            return None
        finally:
            self.close_connect()

    def truncate_table(self, table=None, schema=None):
        AMBIENTE = {0:"PRODUCAO", 1:"HOMOLOGACAO"}
        DATABASE_HOMOLOGACAO = os.environ.get(f"DATABASE_{AMBIENTE[self.ambiente]}")
        USER_HOMOLOGACAO = os.environ.get(f"USER_{AMBIENTE[self.ambiente]}")
        PASSWORD_HOMOLOGACAO = os.environ.get(f"PASSWORD_{AMBIENTE[self.ambiente]}")
        HOST_HOMOLOGACAO = os.environ.get(f"HOST_{AMBIENTE[self.ambiente]}")
        PORT_HOMOLOGACAO = os.environ.get(f"PORT_{AMBIENTE[self.ambiente]}")
        try:
            self.create_connect()
            if query is None:
                try:
                    conn =  psycopg2.connect(database=DATABASE_HOMOLOGACAO, \
                                            user=USER_HOMOLOGACAO, \
                                            password=PASSWORD_HOMOLOGACAO, \
                                            host=HOST_HOMOLOGACAO, \
                                            port=PORT_HOMOLOGACAO)
                    conn.autocommit = True
                    cursor = conn.cursor()
                    print(cursor.execute(f"truncate table {self.schema}.{self.table}"))
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    print('erro')
            else:
                try:
                    conn =  psycopg2.connect(database=DATABASE_HOMOLOGACAO, \
                                            user=USER_HOMOLOGACAO, \
                                            password=PASSWORD_HOMOLOGACAO, \
                                            host=HOST_HOMOLOGACAO, \
                                            port=PORT_HOMOLOGACAO)
                    conn.autocommit = True
                    cursor = conn.cursor()
                    cursor.execute(table=table, schema=schema) 
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: {e}')
            return None
        finally:
            self.close_connect()


