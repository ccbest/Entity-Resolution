
from pathlib import Path
from typing import List

from utils.entlet import Entlet
from skopeutils.drivers.local import LocalFileDriver


def munge(file: Path) -> List[Entlet]:
    # These columns won't be used for anything - ok to drop
    drop_cols = ['iso2', 'iso3', 'code3', 'FIPS', 'Country_Region', 'Combined_Key']
    data = LocalFileDriver().read_file(path=file, headers=True, encoding='utf-8', drop_columns=drop_cols)

    entlets = []

    for row in data:
        if not row["Admin2"] or not row["UID"]:
            continue

        entlet = Entlet()
        uid = row.pop("UID")
        entlet.add({"data_source": "jhu",
                    "unique_id": {
                        "id": uid,
                        "type": "county_fips"
                    },
                    "ent_type": "county",
                    "name": {
                        "name": row.pop("Admin2"),
                        "type": "name"
                    },
                    "state": row.pop("Province_State"),
                    "country": "US",
                    "lat": row.pop("Lat"),
                    "long": row.pop("Long_"),
                    "covid_cases": row})

        entlet.define_individual_uid(uid)

        entlet.submit()

    return entlets
