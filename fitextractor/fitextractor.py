
__all__ = ['FitMessage', 'MessageSummary', 'FitExtractor']

import hashlib
import fitdecode
import typing as t

from dataclasses import dataclass

import pandas as pd
import numpy as np

Fileish: t.TypeAlias = str | t.BinaryIO

@dataclass
class FitField:
    name: str
    value: float
    raw_value: int
    unit: str

@dataclass
class FitMessage:
    name: str
    num_messages: int
    type_map: dict[np.dtype]
    
@dataclass
class MessageSummary:
    infos: dict[str, FitMessage]

    @property
    def names(self) -> list[str]:
        return tuple(self.infos.keys())

class FitExtractor:
    """Uses fitdecode to deconstruct fit files"""

    def __init__(self, file: Fileish, include_unknown: bool = False) -> None:
        self._file: Fileish = file

        # Config
        self._include_unknown: bool = include_unknown
        
        # Included in the fit file
        self._header: fitdecode.records.FitHeader = None
        self._crc: fitdecode.records.FitCRC = None
        self._messages: dict[str, list[list[FitField]]] = {}

        # Extra
        self._md5_hash: str = None
        self._summary: MessageSummary = None
        self._processed: bool = False

    @property
    def file(self) -> Fileish:
        """Returns the input file"""
        return self._file

    @property
    def header(self) -> fitdecode.records.FitHeader:
        """Returns the messages from the fitfile"""
        self._ensure_processed()
        return self._header
    
    @property
    def messages(self) -> list[dict]:
        """Returns the messages from the fitfile"""
        self._ensure_processed()
        return self._messages
    
    @property
    def crc(self) -> fitdecode.records.FitCRC:
        """Returns the messages from the fitfile"""
        self._ensure_processed()
        return self._crc
    
    @property
    def md5_hash(self) -> str:
        """Returns the md5 hash for the fitfile"""
        if self._md5_hash is None:
            self._calc_hash()
        return self._md5_hash
    
    @property
    def summary(self) -> MessageSummary:
        """Returns a summar of the messages"""
        
        # Create summary
        if self._summary is None:
            self._create_summary()
        
        return self._summary
    
    def _calc_hash(self, chunksize: int = 4096) -> str:
        """Calculates an md5 hash for the fitfile"""
        hash_md5 = hashlib.md5()
        with open(self._file, "rb") as f:
            for chunk in iter(lambda: f.read(chunksize), b""):
                hash_md5.update(chunk)
        self._md5_hash = hash_md5.hexdigest()
        return self._md5_hash

    def _process(self) -> None:
        """ Processes the fit file according to the settings and places in to class props"""
        if not self._file:
            raise Exception('No file specified')
        
        # Extract messages and more
        with fitdecode.FitReader(self._file) as fit:
            for frame in fit:
                
                if frame.frame_type == fitdecode.FIT_FRAME_HEADER:
                    self._header = frame
                elif frame.frame_type == fitdecode.FIT_FRAME_CRC:
                    self._crc = frame
                elif frame.frame_type == fitdecode.FIT_FRAME_DEFINITION:
                    pass
                elif frame.frame_type == fitdecode.FIT_FRAME_DATA:

                    if not self._include_unknown and 'unknown' in frame.name:
                        continue

                    if frame.name not in self._messages and (frame.name):
                        self._messages[frame.name] = []

                    fields_clean = [FitField(field.name, field.value, field.raw_value, field.units) for field in frame.fields if ('unknown' not in field.name) or self._include_unknown]
                    if len(fields_clean):
                        self._messages[frame.name].append(fields_clean)

                elif frame.frame_type == fitdecode.FIT_FRAME_DEFMESG:
                    pass
                elif frame.frame_type == fitdecode.FIT_FRAME_DATAMESG:
                    pass

        self._processed = True

    def _ensure_processed(self) -> None:
        """Helper to call process"""
        if not self._processed:
            self._process()

    def manual_process(self) -> None:
        """Process the fit file and return self"""
        self._ensure_processed()

    def _create_summary(self) -> None:
        self._ensure_processed()
        message_infos = {}
        for message_name in self._messages.keys():
            df = self.get_message_df(message_name)
            message_infos[message_name] = FitMessage(message_name, df.shape[0], {col: dtype.name for col, dtype in zip(df.columns, df.dtypes)})
            
        self._summary = MessageSummary(message_infos)
    
    
    def get_message_df(self, name: str, target_attr: str = 'value') -> pd.DataFrame:
        """Get a dataframe with the message type name"""
        self._ensure_processed()
        if name in self._messages:
            row_data = [{field.name: getattr(field, target_attr) for field in message_fields} for message_fields in self._messages[name]]
            return pd.DataFrame(row_data).dropna(axis=1, how='all') # Drop columns with all na -> leads to strange dtypes
        else:
            raise Exception(f'{name} not in file messages')

if __name__ == '__main__':
    pass