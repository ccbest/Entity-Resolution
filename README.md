# Entity Resolution

## What is it?

Entity resolution is a Python package that provides fast, extensible methods for applying complex logic in order to 
merge and transitively link records between disparate datasets.

## Main Features

* Create proto-entities ("entlets") representing entities from singular data sources
* Define complex "Strategies" to determine rulesets by which records should merge
* Create an entity resolution pipeline 

## Simple Example
University of Leipzig's [DBLP-ACM dataset](https://dbs.uni-leipzig.de/research/projects/object_matching/benchmark_datasets_for_entity_resolution)
provides two files, both describing published papers, with similar columns:
* A unique id (scoped to the file)
* A title
* A list of authors
* A venue
* A year

The titles vary slightly between files and different authors may be listed for a given paper. There's no common unique
id to execute a `JOIN` on, and there may even be duplicates that are only slightly different.

Entity Resolution lets you specify a set of complex comparison metrics, called a "strategy", for any combination of 
features present in the data. For example, you may want to use the following rules:
* `title` using Levenshtein distance
* `authors` using Jaccard ratio of tokens
* `year` using exact match

## In Depth examples
For more in depth workflows and explanations of the methodology, reference the [notebooks](./notebooks) folder.

## Install
Install the latest version of Entity Resolution:
```shell
$ pip install entity-resolution
```

## Research
Entity Resolution is (most notably) inspired by the below publications:
* [Collective Entity Resolution in Relational Data](https://www.norc.org/pdfs/may%202011%20personal%20validation%20and%20entity%20resolution%20conference/collective%20entity%20resolution%20in%20relational%20data_pverconf_may2011.pdf)
* [Comparative Analysis of Approximate Blocking Techniques for Entity Resolution](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf)


## License
Released under standard MIT license (see LICENSE.txt):
```
Copyright (c) 2021 entity-resolution Developers
Carl Best
Jessica Moore
```