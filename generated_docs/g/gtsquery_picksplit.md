# gtsquery_picksplit

## Location
src/backend/utils/adt/tsquery_gist.c: 167 - 272

## Overview
A GiST picksplit function for TSQuery indexes that implements the node splitting algorithm by finding optimal seeds and distributing entries based on Hamming distance calculations to maintain index efficiency.

## Definition


## Detailed Description
The gtsquery_picksplit function implements the picksplit method for GiST (Generalized Search Tree) indexes on TSQuery data types. This function is responsible for splitting an overfull index node into two nodes when an insertion would exceed the node's capacity.

The algorithm works in several phases:
1. **Seed Selection**: Iterates through all pairs of entries to find the two entries with maximum Hamming distance (most dissimilar signatures) to serve as seeds for the two new nodes.
2. **Cost Vector Creation**: Calculates the cost of assigning each entry to either seed based on the absolute difference of Hamming distances to each seed.
3. **Sorting**: Uses qsort with comparecost to sort entries by assignment cost, processing entries with clearest preferences first.
4. **Distribution**: Assigns each entry to the closer seed, with a bias factor (WISH_F) to maintain balanced node sizes.
5. **Union Calculation**: Updates the node signatures by performing bitwise OR operations as entries are assigned.

This sophisticated approach ensures that similar TSQuery signatures are grouped together while maintaining reasonably balanced node sizes, which is crucial for query performance.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro for function arguments:
  - Argument 0: GistEntryVector pointer containing all entries to be split
  - Argument 1: GIST_SPLITVEC pointer to store the split result

## Dependencies
- Functions called/Symbols referenced:
  - [GistEntryVector](../G/GistEntryVector.md) (vector of GiST entries)
  - [GIST_SPLITVEC](../G/GIST_SPLITVEC.md) (structure to hold split results)  
  - TSQuerySign (TSQuery signature type)
  - SPLITCOST (structure for split cost calculation)
  - [hemdist](../h/hemdist.md) (Hamming distance calculation between signatures)
  - GETENTRY (macro to extract entry from vector)
  - [comparecost](../c/comparecost.md) (comparison function for sorting)
  - qsort (standard library sort function)
  - WISH_F (macro for balancing bias factor)
  - [TSQuerySignGetDatum](../T/TSQuerySignGetDatum.md) (converts TSQuery signature to Datum)
  - FirstOffsetNumber, OffsetNumberNext (offset number utilities)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
- Called from:
  - No direct references found (likely called through GiST function table)

## Notes and Other Information
- This is a PostgreSQL function following the fmgr interface convention
- Part of the GiST access method implementation for TSQuery data types
- The algorithm balances signature similarity with node size considerations
- Uses a sophisticated cost-based approach rather than simple distance-based assignment
- The WISH_F bias factor helps prevent highly unbalanced splits
- Critical for maintaining good GiST index performance during insertions
- Located in src/backend/utils/adt/tsquery_gist.c:167-272
- Returns a Datum containing the GIST_SPLITVEC structure with split results