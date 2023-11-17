
from fitextractor import MultiFitProcessor 
import glob

if __name__ == '__main__':

    filenames = glob.glob("fit_data_clean/**.fit")[:500]

    mfe = MultiFitProcessor(filenames, multiprocessing=True)

    sqlite = False
    if sqlite:
        mfe.to_db(db_url = 'sqlite:///test.db', drop_tables=True)
    else:
        mfe.to_db(drop_tables=True)