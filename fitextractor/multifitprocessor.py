

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

    def __init__(self, files: list[str], multiprocessing: bool = True) -> None:  
        self._multiprocessing: bool = multiprocessing
        self._fes: list[FitExtractor] = [FitExtractor(file) for file in files]

    @property
    def fes(self) -> list[FitExtractor]:
        """FitExtractors for the loaded fit files"""
        return self._fes
    
    @fes.setter
    def fes(self, value):
        self._fes = value

    def _do_processing(self, inputs: tuple[tuple], function: t.Callable) -> list:
        """Internal handler to either do things in parallel or loopy"""
        if self._multiprocessing:
            with Pool(os.cpu_count()) as pool:
                res = pool.starmap(function, inputs)
        else:
            res = list(function(*input) for input in inputs)
        return res
    
    def _process_single_manual(self, i: int, n: int, fe: FitExtractor) -> FitExtractor:
        fe.manual_process()
        print(f".fit {i+1:6.0f} / {n:<6.0f} processed")
        return fe

    def process_all_manual(self) -> None:
        """Processes all fit files and generate the message data"""
        print("Processing all files:")
        n = len(self.fes)
        return self._do_processing(tuple((i, n, fe) for i, fe in enumerate(self.fes)), self._process_single_manual)

    def get_message_names(self, fes: list[FitExtractor]) -> tuple[str]:
        """Gets a list of the message names found in the loaded fit files"""
        return tuple(set(name for fe in fes for name in fe.summary.names))
    
    def get_message_types(self, fes: list[FitExtractor]) -> dict[str,dict[str,tuple[str]]]:
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
        mns = self.get_message_names(fes)
        res = {}
        # Loop all message types
        for mn in mns:
            # Create df with cols as field names and rows as the dtypes of each file that has this message type
            mtypes = pd.DataFrame(tuple(fe.summary.infos[mn].type_map for fe in fes if mn in fe.summary.infos))
            # Create set with all unique not na types
            res[mn] = {col: tuple(set(mtypes[col].dropna().values)) for col in mtypes.columns.values}
        return res
    
    def _assign_field_sql_dtype(self, types) -> sqlalchemy.types.TypeEngine:
        """Takes in a set of dtype names seen for a field across multiple extractors"""
        TYPE_MAPPING = (
            (pd.api.types.is_any_real_numeric_dtype, 'Double'),
            (pd.api.types.is_datetime64_any_dtype, 'DateTime'),
            (pd.api.types.is_string_dtype, 'Text'),
            (pd.api.types.is_object_dtype, 'PickleType')
        )
        prio_res = 0
        type_res = 'PickleType' # Default if none of above
        for type in types: 
            for prio, (fun, val) in enumerate(TYPE_MAPPING):
                if prio >= prio_res and fun(type):
                    type_res = val
                    prio_res = prio
        return getattr(sqlalchemy.types, type_res)

    def _generate_message_sql_dtype_map(self, fes: list[FitExtractor]) -> dict[str, dict[str, sqlalchemy.types.TypeEngine]]:
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

        names = self.get_message_names(fes)
        types = self.get_message_types(fes)

        message_sql_dtype_map = {name: {field: self._assign_field_sql_dtype(field_types) for field, field_types in types[name].items()} for name in names}

        for n, m in message_sql_dtype_map.items():
            if 'timestamp' in m:
                if m['timestamp'] != sqlalchemy.types.DateTime:
                    for fe in fes:
                        if n in fe.summary.names:
                            if fe.get_message_df(n).timestamp.dtype.name != 'datetime64[ns, UTC]': 
                                print('asdf')

        return message_sql_dtype_map
    
    def _create_engine(self, db_url: str) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(db_url)
    
    def _drop_tables(self, db_url: str) -> None:
        "Drop the tables"

        engine = self._create_engine(db_url)
        
        with engine.connect() as con:
            con.commit()

        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=engine)
        metadata.drop_all(bind=engine)

        engine.dispose()

    def _create_tables(self, type_sql_map: dict, db_url: str) -> sqlalchemy.Table:
        """Creates the required tables using the type mapping information"""

        engine = self._create_engine(db_url)

        def print_table(t: sqlalchemy.Table) -> None:
            print(f"New table: {t.name}")
            for c in t.columns:
                print(f"\t{c.name:>40s} : {str(c.type):<40s}")
            print()


        uuid_sql_type = sqlalchemy.types.UUID if 'sqlite' not in db_url else sqlalchemy.types.String(36)

        meta = sqlalchemy.MetaData()
        fitfile_table = sqlalchemy.Table(
            "fitfiles",
            meta,
            sqlalchemy.Column("uuid", uuid_sql_type, primary_key=True),
            sqlalchemy.Column("filename", sqlalchemy.types.String(255), nullable=False),
            sqlalchemy.Column("md5_hash", sqlalchemy.types.String(32), nullable=False),
            sqlalchemy.Column("message_types", sqlalchemy.types.ARRAY(sqlalchemy.types.String(255)) if 'sqlite' not in db_url else sqlalchemy.types.String(36), nullable=False),
            sqlalchemy.Column("blob", sqlalchemy.types.LargeBinary, nullable=False)
        )
        fitfile_table.create(engine)
        print_table(fitfile_table)

        for message_name, field_types in type_sql_map.items():
            columns = tuple(sqlalchemy.Column(field, field_type) for field, field_type in field_types.items())
            t_name = f"message_{message_name}"
            table = sqlalchemy.Table(
                t_name,
                meta,
                sqlalchemy.Column("fitfile_uuid", uuid_sql_type, sqlalchemy.ForeignKey('fitfiles.uuid')),
                sqlalchemy.Column('index', sqlalchemy.types.BIGINT),
                *columns)
            table.create(engine)
            print_table(table)
        
        engine.dispose()

        return fitfile_table

    def _add_fit_message_data_to_table(self, i: int, n: id, fe: FitExtractor, type_sql_map: dict, fitfile_table: sqlalchemy.Table, db_url: str) -> None:
        """The meat and potatoes - take a fit extractor and load to DB """
        
        engine = self._create_engine(db_url)

        with open(fe.file, 'rb') as f:
            data_blob = f.read()

        fitfile_uuid = uuid.uuid4()

        statement = fitfile_table.insert().values(
                        uuid=fitfile_uuid if 'sqlite' not in db_url else str(fitfile_uuid),
                        filename=fe.file.split('/')[-1],
                        md5_hash=fe.md5_hash,
                        message_types=fe.summary.names if 'sqlite' not in db_url else str(fe.summary.names),
                        blob = data_blob
                    )
        
        with engine.connect() as con:
            con.execute(statement)
            con.commit()

        for message_name, dtype_map in type_sql_map.items():
            if message_name in fe.summary.names:
                try:
                    df = fe.get_message_df(message_name)
                    df["fitfile_uuid"] = fitfile_uuid if 'sqlite' not in db_url else str(fitfile_uuid)
                    df.to_sql('message_' + message_name, engine, if_exists="append", dtype=dtype_map)
                    print(f".fit {i+1:6.0f} / {n:<6.0f} {df.shape[0]:6.0f} rows -> {message_name}")
                except Exception as e:
                    print(f".fit {i+1:6.0f} / {n:<6.0f} {df.shape[0]:6.0f} ERROR IN DB INSERT")
                    print('-'*50)
                    print(e)
                    print('-'*50)
        
        engine.dispose()

    def to_db(self, db_url: str = DEFAULT_DB_URL, drop_tables: bool = False) -> None:
        """Processes the loaded fit files and insert in DB"""
        
        print("Preprocessing files to get data type information for table creation\n")
        fes_proc = self.process_all_manual()

        print("Creating table type maps...")
        type_sql_map = self._generate_message_sql_dtype_map(fes_proc)

        # Dropping tables - if desired
        if drop_tables:
            self._drop_tables(db_url)

        # Create data tables
        file_table = self._create_tables(type_sql_map, db_url)

        # Load in the data
        print("Start inserting data in tables")
        n = len(fes_proc)
        res = self._do_processing(((i, n, fe, type_sql_map, file_table, db_url) for i, fe in enumerate(fes_proc)), self._add_fit_message_data_to_table)
    
if __name__ == '__main__':
   pass