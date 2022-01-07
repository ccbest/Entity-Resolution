## Entity Resolution Overview

#### What is Entity Resolution (ER)?
In a nutshell, ER is the process of coalescing complex data using sophisticated join logic. 

Imagine you have two records from two datasets, both representing Springfield, Illinois, that look something like this:
```json
  {
    "name": "Springfield",
    "state": "IL",
    "coords": {
      "lat": "89.6501",
      "lon": "39.7817"
    }
  }
```
```json
  {
    "name": "Springfield",
    "state": "Illinois",
    "coords": {
      "lat": "89.65",
      "lon": "39.78"
    }
  }
```
Having two representations of the same thing isn't great, but there's no obvious way to "join" these records together in a SQL-like fashion (34 states have a town named Springfield). Instead, you'll probably have to do some data cleaning - maybe standardize the value "Illinois" as "IL" or enforce a certain number of digits in a coordinate.

In reality, joining together datasets is almost never this easy - things are misspelled, lat/lon pairs get reversed, tables have all kinds of insane structure, etc. So even after spending hours writing custom scripts to clean the data, merging everything together will still be expensive. 

That's where ER comes in. With ER, you can define how to merge data structures using any logic you want. These 'rules' for merging records are called 'strategies'. A few examples of strategies for the above examples might be:
* Name exact match, and State (standardized) exact match
* Name has levenshtein distance < 3, and State (standardized) exact match
* Name has levenshtein distance < 3, State (standardized) exact match, and lat/lon within 100km of each other

If any one of these 'strategies' succeeds, the data structures will be merged in post-resolution so that the output looks something like this:
```json
  {
    "name": ["Springfield"],
    "state": ["Illinois", "IL"],
    "coords": [
      {
          "lat": "89.65",
          "lon": "39.78"
      },
      {
          "lat": "89.6501",
          "lon": "39.7817"
      }
    ]
  }
```

