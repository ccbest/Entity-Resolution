
from pathlib import Path

from skopeutils.download_file import download_file
from definitions import DATA_REPO


def fetch():
    download_loc = 'https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv'

    file_loc = download_file(
        download_loc,
        Path(DATA_REPO, "fetch", "usafacts")
    )

    return file_loc

