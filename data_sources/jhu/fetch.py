
from pathlib import Path
from typing import List

from definitions import DATA_REPO
from skopeutils.download_file import download_file


def fetch() -> List[Path]:
    download_loc = 'https://github.com/CSSEGISandData/COVID-19/archive/master.zip'
    data_file_loc = './COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/' \
                    'time_series_covid19_confirmed_US.csv'

    loc = download_file(
        download_loc,
        Path(DATA_REPO, "fetch", "jhu"),
        extricate_files=[Path(data_file_loc)],
    )

    return loc

