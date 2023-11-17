
from fitextractor import MultiFitProcessor 
import glob

if __name__ == '__main__':

    filenames = glob.glob("fit_data_clean/**.fit")[:10]

    sqlite = True
    if sqlite:
        db_url = 'sqlite:///test.db'
        mfe = MultiFitProcessor(filenames, db_url, multiprocessing=False)
    else:
        mfe = MultiFitProcessor(filenames)

    mfe.to_db(drop_tables=True)