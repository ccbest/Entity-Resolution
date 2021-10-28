
from pathlib import Path
from typing import List

from utils.entlet import Entlet
from skopeutils.drivers.local import LocalFileDriver


def munge(file: Path) -> List[Entlet]:
    # These columns won't be used for anything - ok to drop
    drop_cols = ['New_cases', 'New_deaths']

    entlets = []

    entlet = Entlet()
    covid_cases = {}
    covid_deaths = {}

    curr_country_code = None

    for i, row in enumerate(
            LocalFileDriver().read_file(path=file, headers=True, encoding='utf-8', drop_columns=drop_cols)
    ):
        if i == 0:
            curr_country_code = row["Country_code"]

        if not row["Country_code"]:
            continue

        # Country code change means need to create a new entlet
        if curr_country_code != row["Country_code"]:
            entlet.add(
                {
                    "ent_type": "country",
                    "name": [
                        {
                            "name": row["Country"],
                            "type": "name"
                        },
                        {
                            "name": row["Country_code"],
                            "type": "iso_2"
                        }
                    ],
                    "covid_cases": covid_cases,
                    "covid_deaths": covid_deaths,
                    "data_source": "who"
                }
            )
            entlet.define_individual_uid(curr_country_code)

            entlets.append(entlet)

            entlet = Entlet()
            covid_cases = {}
            covid_deaths = {}

            if row["Country_code"]:
                curr_country_code = row["Country_code"]

        date_fix = row["Date_reported"].split("-")
        date_fix = f"{date_fix[1]}/{date_fix[2]}/{date_fix[0]}"

        covid_cases[date_fix] = row["Cumulative_cases"]
        covid_deaths[date_fix] = row["Cumulative_deaths"]

    return entlets
