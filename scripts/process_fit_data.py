
from fitextractor import MultiFitProcessor 
import glob

if __name__ == '__main__':

    filenames = glob.glob("fit_data_clean/**.fit")#[-5500:-5450]

    mfp = MultiFitProcessor(filenames, multiprocessing=True)

    sqlite = False
    if sqlite:
        mfp.to_db(db_url = 'sqlite:///test.db', drop_tables=True)
    else:
        mfp.to_db(drop_tables=True)