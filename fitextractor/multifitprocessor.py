

__all__ = ['MultiFitProcessor']

import os
import uuid

import typing as t
from multiprocessing import Pool

import sqlalchemy
import pandas as pd

from fitextractor import FitExtractor

DEFAULT_DB_URL = 'postgresql://postgres@localhost:5432/fitdata'

class MultiFitProcessor:
    """Get data from multiple .fit files into a DB.
    
    Uses multiple FitExtractors and some duct tape code to create and populate SQL databases.
    """

    def __init__(self, files: list[str], db_url: str = DEFAULT_DB_URL, multiprocessing: bool = True) -> None:  
        self._db_url = db_url      
        self._multiprocessing: bool = multiprocessing
        self._fes: list[FitExtractor] = [FitExtractor(file) for file in files]

    @property
    def fes(self) -> list[FitExtractor]:
        """FitExtractors for the loaded fit files"""
        return self._fes

    def _do_processing(self, inputs: tuple[tuple], function: t.Callable) -> list:
        """Internal handler to either do things in parallel or loopy"""
        if self._multiprocessing:
            with Pool(os.cpu_count()) as pool:
                res = pool.starmap(function, inputs)
        else:
            res = list(function(*input) for input in inputs)
        return res
    
    def _process_single_manual(self, fe: FitExtractor) -> FitExtractor:
        fe.manual_process()
        return fe

    def process_all_manual(self) -> list[FitExtractor]:
        """Processes all fit files and generate the message data"""
        return self._do_processing(tuple((fe,) for fe in self.fes), self._process_single_manual)

    def get_message_names(self) -> tuple[str]:
        """Gets a list of the message names found in the loaded fit files"""
        return tuple(set(name for fe in self.fes for name in fe.summary.names))
    
    def get_message_types(self) -> dict[str,dict[str,tuple[str]]]:
        """Gets a mapping of the message names to fields and their datatypes, by looking through all the loaded fit files.

        The field's datatypes are sets of all the seen types through the files.
        Types are the str names of the dataframe dtypes generated using FitExtractor.get_message_df
        
        Structure is {
            message_name1: {
                field_name1: (type1, type2),
                field_name2: (type1, type3),
                field_name3: (type2)
            },
            message_name2: {
                ...
            }
        }
        """
        mns = self.get_message_names()
        res = {}
        # Loop all message types
        for mn in mns:
            # Create df with cols as field names and rows as the dtypes of each file that has this message type
            mtypes = pd.DataFrame(tuple(fe.summary.infos[mn].type_map for fe in self.fes if mn in fe.summary.infos))
            # Create set with all unique not na types
            res[mn] = {col: tuple(set(mtypes[col].dropna().values)) for col in mtypes.columns.values}
        return res
    
    def _assign_field_sql_dtype(self, types) -> sqlalchemy.types.TypeEngine:
        """Takes in a set of dtype names seen for a field across multiple extractors"""
        TYPE_MAPPING = (
            (pd.api.types.is_datetime64_any_dtype, 'DateTime'),
            (pd.api.types.is_any_real_numeric_dtype, 'Double'),
            (pd.api.types.is_object_dtype, 'PickleType')
        )
        prio_res = 0
        type_res = 'Text' # Default if none of above
        for type in types: 
            for prio, (fun, val) in enumerate(TYPE_MAPPING):
                if prio >= prio_res and fun(type):
                    type_res = val
                    prio_res = prio
        return getattr(sqlalchemy.types, type_res)

    def _generate_message_sql_dtype_map(self) -> dict[str[dict[str[sqlalchemy.types.TypeEngine]]]]:
        """Gets a mapping of the message names to field and SQLAlchemy types, from all the loaded fit files.
        
        Structure is {
            message_name1: {
                field_name1: sqlalchemy_type1
                field_name2: sqlalchemy_type2
                field_name3: sqlalchemy_type2
            },
            message_name2: {
                ...
            }
        }
        """

        names = self.get_message_names()
        types = self.get_message_types()

        message_sql_dtype_map = {name: {field: self._assign_field_sql_dtype(field_types) for field, field_types in types[name].items()} for name in names}

        return message_sql_dtype_map
    
    def _create_engine(self) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(self._db_url)
    
    def _drop_tables(self) -> None:
        "Drop the tables"

        engine = self._create_engine()
        
        with engine.connect() as con:
            con.commit()

        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=engine)
        metadata.drop_all(bind=engine)

    def _create_tables(self, type_sql_map: dict) -> sqlalchemy.Table:
        """Creates the required tables using the type mapping information"""

        engine = self._create_engine()

        meta = sqlalchemy.MetaData()
        fitfile_table = sqlalchemy.Table(
            "fitfiles",
            meta,
            sqlalchemy.Column("uuid", sqlalchemy.types.UUID if 'sqlite' not in self._db_url else sqlalchemy.types.String(36), primary_key=True),
            sqlalchemy.Column("filename", sqlalchemy.types.String(255), nullable=False),
            sqlalchemy.Column("md5_hash", sqlalchemy.types.String(32), nullable=False),
            sqlalchemy.Column("message_types", sqlalchemy.types.ARRAY(sqlalchemy.types.String(255)) if 'sqlite' not in self._db_url else sqlalchemy.types.String(36), nullable=False),
            sqlalchemy.Column("blob", sqlalchemy.types.LargeBinary, nullable=False)
        )
        fitfile_table.create(engine)

        for message_name, field_types in type_sql_map.items():
            columns = tuple(sqlalchemy.Column(field, field_type) for field, field_type in field_types.items())
            table = sqlalchemy.Table(
                f"message_{message_name}",
                meta,
                sqlalchemy.Column("fitfile_uuid", sqlalchemy.types.UUID if 'sqlite' not in self._db_url else sqlalchemy.types.String(36), sqlalchemy.ForeignKey('fitfiles.uuid')),
                sqlalchemy.Column('index', sqlalchemy.types.BIGINT),
                *columns)
            table.create(engine)

        return fitfile_table

    def _add_fit_message_data_to_table(self, fe: FitExtractor, type_sql_map: dict, fitfile_table: sqlalchemy.Table) -> None:
        """The meat and potatoes - take a fit extractor and load to DB """
        
        engine = self._create_engine()

        with open(fe.file, 'rb') as f:
            data_blob = f.read()

        fitfile_uuid = uuid.uuid4()

        statement = fitfile_table.insert().values(
                        uuid=fitfile_uuid if 'sqlite' not in self._db_url else str(fitfile_uuid),
                        filename=fe.file.split('/')[-1],
                        md5_hash=fe.md5_hash,
                        message_types=fe.summary.names if 'sqlite' not in self._db_url else str(fe.summary.names),
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
                    df["fitfile_uuid"] = fitfile_uuid if 'sqlite' not in self._db_url else str(fitfile_uuid)
                    df.to_sql('message_' + message_name, engine, if_exists="append", dtype=dtype_map)
                except Exception as e:
                    print(e)
                    print(df)

    def to_db(self, drop_tables: bool = False) -> None:
        """Processes the loaded fit files and insert in DB"""
        
        self.process_all_manual()

        type_sql_map = self._generate_message_sql_dtype_map()

        if drop_tables:
            self._drop_tables(type_sql_map)

        file_table = self._create_tables(type_sql_map)

        res = self._do_processing(((fe, type_sql_map, file_table) for fe in self.fes), self._add_fit_message_data_to_table)
    
if __name__ == '__main__':
   pass