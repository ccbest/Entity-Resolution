
from pathlib import Path
from typing import List

from definitions import DATA_REPO
from skopeutils.download_file import download_file


def fetch() -> List[Path]:
    download_loc = 'https://www2.census.gov/programs-surveys/acs/summary_file/2020/data/1_year_data_profiles/County'
    download_files = [f'{fname}.csv' for fname in ('DP02', 'DP02PR', 'DP03', 'DP04', 'DP05')]

    destinations = []
    for file in download_files:

        destinations.append(
            *download_file(f'{download_loc}/{file}',
                           Path(DATA_REPO, "fetch", "acs")
            )
        )

    return destinations
