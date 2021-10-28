
from pathlib import Path
import re
from typing import List

from utils.entlet import Entlet
from skopeutils.drivers.local import LocalFileDriver


def munge(file: Path) -> List[Entlet]:
    entlets = []

    Entlet.define_source_uid_field('countyFIPS')

    # process csv in chunks of 1000 rows
    for row in LocalFileDriver().read_file(path=file, headers=True, encoding='utf-8'):
        entlet = Entlet()

        entlet.add({
            "unique_id": {
                "id": row['countyFIPS'],
                "type": "fips"
            },
            "data_source": "usafacts",
            "ent_type": "county",
            "name": {
                "name": row['County Name'],
                "type": "name"
            },
            "state": row['State'],
            "country": "US",
            "covid_cases": {key: val for key, val in row.items() if re.match(r'\d{1,2}/\d{1,2}/\d{4}', key)}
        })
        entlets.append(entlet)

    return entlets
