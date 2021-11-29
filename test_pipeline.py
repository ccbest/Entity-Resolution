
from pathlib import Path
import operator

from resolver import Entlet, EntletMap, Pipeline
from resolver.standardizers import UsState2Code


# file = Path("/Users/carlbest/Desktop/malak/covid_confirmed_usafacts.csv")
# entlets = munge(file)

Entlet.define_source_uid_field("countyFIPS")
entlet = Entlet()
entlet.add({
    "ent_type": "county",
    "data_source": "test",
    "countyFIPS": "12345",
    "name": "Lake County",
    "location": {
        "country": "US",
        "state": "Illinois"
    }
})
entlet.add({
    "location": {
        "country": "UK",
        "state": "Not alabama"
    }
})
emap = EntletMap([entlet])

entlet = Entlet()
entlet.add({
    "ent_type": "county",
    "data_source": "test",
    "countyFIPS": "23456",
    "name": "Lake",
    "location": {
        "country": "US",
        "state": "IL"
    }
})
emap.add(entlet)

entlet = Entlet()
entlet.add({
    "ent_type": "county",
    "data_source": "test",
    "countyFIPS": "34567",
    "name": "DuPage County",
    "location": {
        "country": "US",
        "state": "IL"
    }
})
emap.add(entlet)

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
from resolver.scoring import VectorMagnitude

blocker = SortedNeighborhood("name", window_size=3)
tfidf = TfIdfTokenizedVector()
sim = CosineSimilarity("name", transforms=[tfidf])
sim2 = ExactMatch("location.state")

strategy = Strategy(
    blocker=blocker,
    metrics=[sim, sim2],
    scoring_method=VectorMagnitude(min=1.0)
)


pipeline = Pipeline([strategy], [state_std])

self = pipeline
entlet_df = emap.to_dataframe()

# Standardize stage
entlet_df = self.standardize_entlets(entlet_df, self.standardizers)

entlet_df = self.standardize_entlets(entlet_df, self.standardizers)
fragments = self.fragment(entlet_df, strategy.fragment_fields)

for metric in strategy.metrics:
    fragments = metric.transform(fragments)

blocked = strategy.blocker.block(fragments)

self = strategy
for metric in self.metrics:
    blocked[metric.field_name] = blocked.apply(metric.run, axis=1)





