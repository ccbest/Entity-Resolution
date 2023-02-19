# ResolvER

ResolvER is an extensible framework for building Entity Resolution pipelines in order to merge datasets around "things" based on complex join logic and transitive linking.

Entity Resolution is a complex and computationally expensive process. ResolvER seeks to provide tools that cover the majority of use cases, the ability to enhance those tools with machine learning, and leverage developers' experiential knowledge of data to provide a flexible and efficient solution to the Entity Resolution problem.

## Quick/Simple Example
The University of Leipzig provides [test datasets](https://dbs.uni-leipzig.de/research/projects/object_matching/benchmark_datasets_for_entity_resolution) for Entity Resolution, let's say you're working with the DBLP-ACM dataset.

The dataset provides two files, both describing published papers, with similar columns:
* A unique id (unique to that file only)
* A title
* A list of authors
* A venue
* A year

The titles vary slightly between files, and different authors may be listed for a given paper - in short, there's no clean or consistent way to deduplicate the data.

Using ResolvER, you can specify any number of complex operations to determine whether two given records are duplicates of each other in what are called "Strategies." One such strategy may be:
* `title` has Levenshtein ratio of at least 0.9 AND
* `authors` has Jaccard distance of at least 0.4 AND
* `year` is an exact match

## In Depth examples
For more in depth workflows and explanations of the methodology, reference the [notebooks](./notebooks) folder.

## Install
Install the latest version of ResolvER:
```shell
$ pip install entity-resolution
```

## Research
ResolvER is (most notably) inspired by the below publications:
* [Collective Entity Resolution in Relational Data](https://www.norc.org/pdfs/may%202011%20personal%20validation%20and%20entity%20resolution%20conference/collective%20entity%20resolution%20in%20relational%20data_pverconf_may2011.pdf)
* [Comparative Analysis of Approximate
Blocking Techniques for Entity Resolution](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf)


## License
Released under standard MIT license (see LICENSE.txt):
```
Copyright (c) 2021 entity-resolution Developers
Carl Best
Jessica Moore
```