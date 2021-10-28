# ResolvER
## Distributed Entity Resolution
ResolvER is a fully-distributed Entity Resolution engine designed to merge any number of disparate 
datasets into one coherent picture.

### What is Entity Resolution?
Entity Resolution is the process of de-duplicating records from different sources that are representative 
of the same thing, even when no shared key exists between them, and merging the results into a single, 
complex data structure. In other words, you no longer have to execute page-long joins to bring all of your
data under one hood - all data relevant to the record you need is stored on a single document.

Traditionally, this process has involved exorbitant investments in data preparation and standardization, 
exponentially-increasing runtime complexities based on the size of the data, and difficult-to-consume
results that require intimate knowledge of the process's inner workings. These are the issues that ResolvER
aims to solve.

ResolvER offers a template object (the Entlet) that reduces the barrier to entry down to the beginner-python 
level, flexible configuration and verbose feedback to give the developers full control over the 
resolution process, and drivers for pushing results to the database of your choice (in development).

### How does Entity Resolution work?


## Getting Started
Requirements for running a pipeline can be summarized in two points:
1. You must write a fetcher/parser that converts your data into Entlet objects
2. You must provide "strategies" for resolution that act as deduplication rulesets

#### 1. Writing a parser
<i>Note: many helpers are included for this stage. Refer to [XXXXXX] for more information.


#### 2. Creating resolution strategies

