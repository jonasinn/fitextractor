

__all__ = ['MultiFitProcessor']

import glob
import os
import time
import uuid

import typing as t
from multiprocessing import Pool
from dataclasses import dataclass

import psycopg2
import sqlalchemy
import pandas as pd

from fitextractor import FitExtractor

@dataclass
class DbConfig:
    user: str = 'postgres'
    password: str = ''
    name: str = 'fitdata'
    host: str = 'localhost'
    port: int = 5432
    dialect_driver: str = 'postgresql'

class MultiFitProcessor:
    """Get data from multiple .fit files into a DB.
    
    Uses multiple FitExtractors and some very rough code to get it in to a postgres DB
    """

    def __init__(self, files: list[str], dbconfig = DbConfig(), multiprocessing: bool = True) -> None:        
        self._dbconfig = dbconfig
        self._multiprocessing: bool = multiprocessing
        self.fes: list[FitExtractor] = [FitExtractor(file) for file in files]

    def _do_processing(self, inputs: list, function: t.Callable) -> list:
        if self._multiprocessing:
            with Pool(os.cpu_count()) as pool:
                res = pool.map(function, inputs)
        else:
            res = list(function(input) for input in inputs)
        return res
    
    def _process_single_manual(self, fe: FitExtractor) -> FitExtractor:
        fe.manual_process()
        return fe

    def process_all_manual(self):
        """Processes all fit files and generate the message data"""
        self._do_processing(self.fes, self._process_single_manual)

    def get_message_names(self):
        return tuple(set(name for fe in self.fes for name in fe.summary.names))
    
    def get_message_types(self):
        mns = self.get_message_names()
        res = {}
        # Loop all message types
        for mn in mns:
            # Create df with cols as field names and rows as the dtypes of each file that has this message type
            mtypes = pd.DataFrame(tuple(fe.summary.infos[mn].type_map for fe in self.fes if mn in fe.summary.infos))
            # Create set with all unique not na types
            res[mn] = {col: tuple(set(mtypes[col].dropna().values)) for col in mtypes.columns.values}
        return res
    
    def _assign_field_sql_dtype(self, types):
        """Takes in a set of dtypes seen for a field across multiple extractors"""
        type_mapping = (
            (pd.api.types.is_datetime64_any_dtype, 'TIMESTAMP'),
            (pd.api.types.is_any_real_numeric_dtype, 'DOUBLE_PRECISION'),
            (pd.api.types.is_object_dtype, 'TEXT')
        )
        prio_res = 0
        type_res = 'TEXT' # Default if none of above
        for type in types: 
            for prio, (fun, val) in enumerate(type_mapping):
                if prio >= prio_res and fun(type):
                    type_res = val
                    prio_res = prio
        return getattr(sqlalchemy.types, type_res)

    def generate_message_sql_dtype_map(self):
        names = self.get_message_names()
        types = self.get_message_types()

        message_sql_dtype_map = {name: {field: self._assign_field_sql_dtype(field_types) for field, field_types in types[name].items()} for name in names}

        return message_sql_dtype_map
    
    def _create_engine(self):
        if self._dbconfig.password == '':
            url = f"{self._dbconfig.dialect_driver}://{self._dbconfig.user}@{self._dbconfig.host}:{self._dbconfig.port}/{self._dbconfig.name}"
        else:
            url = f"{self._dbconfig.dialect_driver}://{self._dbconfig.user}:{self._dbconfig.password}@{self._dbconfig.host}:{self._dbconfig.port}/{self._dbconfig.name}"
        return sqlalchemy.create_engine(url)
    
    def _clean_tables(self, type_sql_map: dict):
        con = psycopg2.connect("dbname=fitdata user=jgj")
        cur = con.cursor()
        cur.execute(f"DROP TABLE IF EXISTS fitfiles CASCADE;") # Should drop all
        for name in type_sql_map.keys():
            cur.execute(f"DROP TABLE IF EXISTS message_{name};")
        con.commit()
        con.close()
  
    def _create_tables(self, type_sql_map: dict):

        engine = self._create_engine()

        meta = sqlalchemy.MetaData()
        fitfile_table = sqlalchemy.Table(
            "fitfiles",
            meta,
            sqlalchemy.Column("uuid", sqlalchemy.types.UUID, primary_key=True),
            sqlalchemy.Column("filename", sqlalchemy.types.String(255), nullable=False),
            sqlalchemy.Column("md5_hash", sqlalchemy.types.String(32), nullable=False),
            sqlalchemy.Column("message_types", sqlalchemy.types.ARRAY(sqlalchemy.types.String(255)), nullable=False),
            sqlalchemy.Column("blob", sqlalchemy.types.LargeBinary, nullable=False)
        )
        fitfile_table.create(engine)

        for message_name, field_types in type_sql_map.items():
            columns = tuple(sqlalchemy.Column(field, field_type) for field, field_type in field_types.items())
            table = sqlalchemy.Table(
                f"message_{message_name}",
                meta,
                sqlalchemy.Column("fitfile_uuid", sqlalchemy.types.UUID, sqlalchemy.ForeignKey('fitfiles.uuid')),
                sqlalchemy.Column('index', sqlalchemy.types.BIGINT),
                *columns)
            table.create(engine)

        return fitfile_table

    def _add_fit_message_data_to_table(self, fe: FitExtractor, type_sql_map: dict, fitfile_table: sqlalchemy.Table):
        
        engine = self._create_engine()

        with open(fe.file, 'rb') as f:
            data_blob = f.read()

        fitfile_uuid = uuid.uuid4()

        statement = fitfile_table.insert().values(
                        uuid=fitfile_uuid,
                        filename=fe.file.split('/')[-1],
                        md5_hash=fe.md5_hash,
                        message_types=fe.summary.names,
                        blob = data_blob
                    )
        
        with engine.connect() as con:
            con.execute(statement)
            con.commit()

        for message_name, dtype_map in type_sql_map.items():
            if message_name in fe.summary.names:
                try:
                    print(f"Creating table {message_name} for {fe._file}")
                    df = fe.get_message_df(message_name)
                    df["fitfile_uuid"] = fitfile_uuid
                    df.to_sql('message_' + message_name, engine, if_exists="append", dtype=dtype_map)
                except Exception as e:
                    print(e)
                    print(df)

        return True

    def to_db(self):
        
        self.process_all_manual()

        type_sql_map = self.generate_message_sql_dtype_map()

        self._clean_tables(type_sql_map)

        file_table = self._create_tables(type_sql_map)

        with Pool(os.cpu_count()) as pool:
            res = pool.starmap(self._add_fit_message_data_to_table, ((fe, type_sql_map, file_table) for fe in self.fes))


    
if __name__ == '__main__':
    import glob
    import time
    filenames = glob.glob("fit_data_clean/**.fit")[:50]

    mfe = MultiFitProcessor(filenames)
    
    mfe.to_db()