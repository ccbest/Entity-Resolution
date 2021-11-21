## Blocking in ER

### What is blocking?
Blocking is the logic that determines which fragments get "matched up" for further comparison. For example, blocking by alphabetical neighborhood with a window size of 3 would match fragments with the three fragments alphabetically before and after themselves.

### Why do we need to block fragments?
Two reasons -
1. ER is extremely computationally expensive - comparing every combination of data between 100 entities might not seem like a big deal but things get crazy quickly. If you don't block your fragments appropriately, you'll probably have to wait a long time for your results.
2. Strategies are never perfect - each comparison you run carries a chance that some wildly unlikely and unforeseen circumstances cause two entities to merge that shouldn't have. The larger dataset you work with the more likely this phenomenon becomes, leading to the "black hole" effect.

### Considerations for blocking
#### You can only select one method for blocking per strategy.
Strategies require that all comparisons between fragments pass in order for resolution to occur. If multiple blocking methods were allowed, there's no guarantee that all comparisons would even occur (it's actually highly unlikely). Choose the blocking method that's the most meaningful for the strategy.

