
from toolkit.entlet import Entlet
from toolkit.io.local_read_write import LocalReadWrite


def munge(file):
    # These columns won't be used for anything - ok to drop
    drop_cols = ['iso2', 'iso3', 'code3', 'FIPS', 'Country_Region', 'Combined_Key']
    data = LocalReadWrite.read_2d(path=file, headers=True, encoding='utf-8', drop_columns=drop_cols)

    entlets = []

    for row in data:
        if not row["Admin2"]:
            continue

        entlet = Entlet()
        entlet.add_uid_values(row["UID"])

        entlet.add({"entlet_id": row.pop("UID"),
                    "ent_type": "county",
                    "name": row.pop("Admin2"),
                    "state": row.pop("Province_State"),
                    "lat": row.pop("Lat"),
                    "long": row.pop("Long_"),
                    "covid_cases": row})

        entlets.append(entlet)

    return entlets
