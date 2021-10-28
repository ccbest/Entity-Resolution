
import os
import shutil
from data_sources import download_file
from definitions import DATA_REPO


DOWNLOAD_LOC = 'https://github.com/datasciencecampus/google-mobility-reports-data/archive/master.zip'
DATA_FILE_LOC = 'master/google-mobility-reports-data-master/csvs/international_local_area_trends_UK_DE_FR_ES_IT_US_SE.csv'


def fetch():

    file_loc = download_file(
        DOWNLOAD_LOC,
        os.path.join(DATA_REPO, "fetch", "mobility"),
        file_list=[DATA_FILE_LOC]
    )

    return file_loc

