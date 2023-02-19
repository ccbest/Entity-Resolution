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
    "name": "Sprngfield",
    "state": "Illinois",
    "coords": {
      "lat": "89.65",
      "lon": "39.78"
    }
  }
```
In reality, joining together datasets is almost never this easy - things are misspelled, lat/lon pairs get reversed, tables have all kinds of insane structure, etc. So even after spending hours writing custom scripts to clean the data, merging everything together will still be expensive. 

That's where ER comes in. With ER, you can define how to merge data structures using any logic you want. These 'rules' for merging records are called 'strategies'. A few examples of strategies for the above examples might be:
* Name exact match, and State exact match
* Name has levenshtein distance < 3, and State exact match
* Name has levenshtein distance < 3, State exact match, and lat/lon within 100km of each other

If any one of these 'strategies' succeeds, the data structures will be merged in post-resolution so that the output looks something like this:
```json
  {
    "name": ["Springfield", "Sprngfield"],
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

