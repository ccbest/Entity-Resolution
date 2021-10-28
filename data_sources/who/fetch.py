
from pathlib import Path

from skopeutils.download_file import download_file
from definitions import DATA_REPO


def fetch():
    download_loc = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'

    file_loc = download_file(
        download_loc,
        Path(DATA_REPO, "fetch", "who")
    )

    return file_loc

