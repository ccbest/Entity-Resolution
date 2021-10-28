
from collections import defaultdict
from pathlib import Path

from skopeutils.drivers.local import LocalFileDriver
from utils.entlet import Entlet


def munge(file: Path) -> None:
    # These columns won't be used for anything - ok to drop
    drop_cols = ['TBLID', 'PROFTBL', 'PROFLN', 'PRF_MG_ERROR', 'PCT_ESTIMATE', 'PCT_MG_ERROR']

    active_title = ''
    section_data = defaultdict(dict)
    curr_geoid = None
    entlet = Entlet()

    for i, row in enumerate(
            LocalFileDriver().read_file(path=file, headers=True, encoding='latin-1', drop_columns=drop_cols)
    ):
        if i == 0:
            curr_geoid = row["GEOID"]

        if row["GEOID"] != curr_geoid:
            entlet.define_source_uid(curr_geoid)
            curr_geoid = row["GEOID"]

            entlet.add(section_data)
            section_data = defaultdict(dict)

            entlet.submit()
            entlet = Entlet()

        name, state = [item.strip() for item in row["GEONAME"].split(',')]
        entlet.add({
            "ent_type": "county",
            "name": {
                "name": name,
                "type": "name"
            },
            "state": state,
            "country": "US",
            "data_source": "acs"
        })

        # If there isn't a value, it means there's either a new title or its a blank line
        if not row["PRF_ESTIMATE"]:
            active_title = row["TITLE"] or active_title
            continue

        section_data[active_title][row["TITLE"]] = row["PRF_ESTIMATE"]

    return
