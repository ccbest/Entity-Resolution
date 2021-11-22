
from pathlib import Path
import operator

from resolver import Entlet, EntletMap, Pipeline
from resolver.standardizers import UsState2Code


# file = Path("/Users/carlbest/Desktop/malak/covid_confirmed_usafacts.csv")
# entlets = munge(file)

Entlet.define_source_uid_field("countyFIPS")
test_entlet = Entlet()
test_entlet.add({
    "ent_type": "county",
    "data_source": "test",
    "countyFIPS": "12345782391",
    "name": "Lake County",
    "location": {
        "country": "US",
        "state": "IL"
    }
})
test_entlet.add({
    "location": {
        "country": "UK",
        "state": "Not alabama"
    }
})
test_entlet.add({
    "name": "Seth"
})

emap = EntletMap([test_entlet])
state_std = UsState2Code(
    "location.state",
    filters=[{
            "field_name": "location.country",
            "comparator": operator.eq,
            "value": "US"
    }]
)

from resolver import Strategy
from resolver.blocking import SortedNeighborhood
from resolver.similarity import CosineSimilarity, ExactMatch
from resolver.transforms import TfIdfTokenizedVector

blocker = SortedNeighborhood("name")
tfidf = TfIdfTokenizedVector()
sim = CosineSimilarity("name", transforms=[tfidf])
sim2 = ExactMatch("location.state")

strat = Strategy(
    blocker=blocker,
    metrics=[sim, sim2]
)


pipeline = Pipeline([strat], [state_std])
self = pipeline

entlet_df = emap.to_dataframe()

# Standardize stage
entlet_df = self.standardize_entlets(entlet_df, self.standardizers)

strategy = strat


### STRATEGIES
from resolver.blocking.text import SortedNeighborhood
from resolver.similarity.vectors import CosineSimilarity

