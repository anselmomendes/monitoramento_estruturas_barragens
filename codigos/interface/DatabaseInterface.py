from abc import ABC, abstractclassmethod
import sqlalchemy
from pandas import DataFrame

class DatabaseInterface(ABC):
    @abstractclassmethod
    def __init__(self, schema:str, table:str, ambiente:int):
        pass

    @abstractclassmethod
    def create_connect(self):
        pass

    @abstractclassmethod
    def select_table(self, query:str):
        pass

    @abstractclassmethod
    def refresh_table(self, table:str, schema:str):
        pass

    @abstractclassmethod
    def truncate_table(self, table:str, schema:str):
        pass

    @abstractclassmethod
    def to_table(self, table:str, schema:str) -> bool:
        pass