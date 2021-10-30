
from pathlib import Path

import operator

from data_sources.usafacts.munge import munge

from pre_resolve.standardize.custom_lookup import UsState2Code

from utils.entletmap import EntletMap
from utils.entlet import Entlet

file = Path("/Users/carlbest/Desktop/malak/covid_confirmed_usafacts.csv")
entlets = munge(file)

test_entlet = Entlet()
test_entlet.add({
    "state": "ALABAMA",
    "country": "US",
    "ent_type": "state",
    "data_source": "test",
    "countyFIPS": "12345782391"
})

emap = EntletMap([test_entlet])
state_std = UsState2Code(
    "state",
    filters = {
            "field_name": "country",
            "comparator": operator.eq,
            "value": "US"
        }

)

emap.standardize(state_std)
