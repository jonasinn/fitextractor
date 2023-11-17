import os
import shutil
import glob

import multiprocessing

import fitdecode
from fitextractor import FitExtractor


FOLDER_CLEAN = 'fit_data_clean'
FOLDER_PROBLEM = 'fit_data_problematic'


def sort_file(file):
    try:
        fe = FitExtractor(file)
        fe.summary
        shutil.copy(file, FOLDER_CLEAN)
    except Exception as e:
        print(f"Had issues with: {file}")
        print(e)
        shutil.copy(file, FOLDER_PROBLEM)


def sort_clean_files():

    os.mkdir(FOLDER_CLEAN)
    os.mkdir(FOLDER_PROBLEM)

    files = glob.glob('fit_data/*.fit')

    with multiprocessing.Pool(os.cpu_count()) as p:
        p.map(sort_file, files)

def analyse_problematic_headers():

    files = glob.glob('fit_data_problematic/**.fit')
    print(f"We have {len(files)} problematic files")
    for file in files:
        try:
            # Extract header
            with fitdecode.FitReader(file) as fit:
                for frame in fit:
                    if frame.frame_type == fitdecode.FIT_FRAME_HEADER:
                        print({slot: getattr(frame, slot) for slot in frame.__slots__})
                        break
        except Exception as e:
            print(e)

if __name__ == "__main__":
    
    #sort_clean_files()
    analyse_problematic_headers()