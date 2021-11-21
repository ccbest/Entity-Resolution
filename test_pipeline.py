
from pathlib import Path
import operator

from resolver import Entlet, EntletMap, Pipeline
from resolver.standardizers import UsState2Code


# file = Path("/Users/carlbest/Desktop/malak/covid_confirmed_usafacts.csv")
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
    filters=[{
            "field_name": "country",
            "comparator": operator.eq,
            "value": "US"
    }]
)

pipeline = Pipeline([state_std], [])
self = pipeline

entlet_df = emap.to_dataframe()

# Standardize stage
entlet_df = self.standardize_entlets(entlet_df, self.standardizers)


### STRATEGIES
from resolver.blocking.text import SortedNeighborhood
from resolver.similarity.vectors import CosineSimilarity

