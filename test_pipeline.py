
from pathlib import Path

import operator

from resolver.standardize import UsState2Code

from utils.entletmap import EntletMap
from utils.entlet import Entlet

file = Path("/Users/carlbest/Desktop/malak/covid_confirmed_usafacts.csv")
# entlets = munge(file)

Entlet.define_source_uid_field("countyFIPS")
test_entlet = Entlet()
test_entlet.add({
    "state": "ALABAMA",
    "country": "US",
    "ent_type": "state",
    "data_source": "test",
    "countyFIPS": "12345782391",
    "location": {
        "country": "US",
        "state": "ALABAMA"
    }
})
test_entlet.add({
    "location": {
        "country": "UK",
        "state": "Not alabama"
    }
})

emap = EntletMap([test_entlet])
state_std = UsState2Code(
    "state",
    filters = [{
            "field_name": "country",
            "comparator": operator.eq,
            "value": "US"
    }]
)

state_std.run(test_entlet)

emap.standardize(state_std)
