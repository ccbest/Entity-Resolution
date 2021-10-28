
from utils.entlet import Entlet
from skopeutils.drivers.local import LocalFileDriver

entlets = []


def munge(file) -> None:
    # These columns won't be used for anything - ok to drop
    drop_cols = ['Cumulative_number_for_14_days_of_COVID-19_cases_per_100000']
    data = LocalFileDriver().read_file(path=file, headers=True, encoding='utf-8', drop_columns=drop_cols)

    Entlet.define_source_uid_field('geoId')
    entlet = Entlet()

    covid_cases = {}
    covid_deaths = {}

    curr_geoid = None

    for i, row in enumerate(data):
        if i == 0:
            curr_geoid = row["geoId"]

        if curr_geoid and row["geoId"] != curr_geoid:
            entlet.add(
                {
                    "ent_type": "country",
                    "name": [
                        {
                            "name": row["countriesAndTerritories"],
                            "type": "name"
                        },
                        {
                            "name": row["geoId"],
                            "type": "iso_2"
                        },
                        {
                            "name": row["countryterritoryCode"],
                            "type": "iso_3"
                        }
                    ],

                    "covid_cases": covid_cases,
                    "covid_deaths": covid_deaths,
                    "population": {
                        "population": row["popData2019"],
                        "year": "2019"
                    },
                    "data_source": "ecdc"
                }
            )
            entlet.submit()

            entlet = Entlet()
            curr_geoid = row["geoId"]
            covid_cases = {}
            covid_deaths = {}

        date_fix = f"{row['month']}/{row['day']}/{row['year']}"
        covid_cases[date_fix] = row["cases"]
        covid_deaths[date_fix] = row["deaths"]

    return
