
from pathlib import Path
from typing import List

from definitions import DATA_REPO
from skopeutils.download_file import download_file


def fetch() -> List[Path]:
    # Strangely enough, the file name is actually just "csv"
    download_loc = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'

    file_loc = download_file(
        download_loc,
        Path(DATA_REPO, "fetch", "ecdc"),
        rename={"csv": "ecdc.csv"}
    )

    return file_loc
