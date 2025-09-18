# gtsquery_consistent

## Location
src/backend/utils/adt/tsquery_gist.c: 53 - 88

## Overview
gtsquery_consistent is a GiST consistency checking function that determines whether a TSQuery index entry is consistent with a given query using containment strategies.

## Definition


## Detailed Description
This function implements the consistency checking logic for TSQuery GiST indexes. It compares TSQuerySign signatures to determine if an index entry might contain matches for a given query. The function supports two search strategies: RTContainsStrategyNumber (@@) and RTContainedByStrategyNumber (<@).

The consistency check works by:
1. Extracting the TSQuerySign from the index entry
2. Creating a TSQuerySign from the search query
3. Performing bitwise operations based on the search strategy
4. For leaf entries: exact containment/contained-by checks
5. For internal entries: overlap checks to determine if subtree traversal is needed

All results are marked as requiring rechecking since the signature-based comparison is lossy.

## Parameters / Member Variables
- : Pointer to GISTENTRY containing the indexed TSQuerySign
- : TSQuery value being searched for
- : Search strategy (RTContainsStrategyNumber or RTContainedByStrategyNumber)
- : Boolean pointer set to indicate if recheck is needed (always true)

## Dependencies
- Functions called/Symbols referenced:
  - GISTENTRY (struct type)
  - TSQuery (type)
  - PG_GETARG_TSQUERY (extract TSQuery from function args)
  - StrategyNumber (type)
  - PG_GETARG_UINT16 (extract strategy number)
  - DatumGetTSQuerySign (extract signature from Datum)
  - TSQuerySign (signature type)
  - makeTSQuerySign (create signature from TSQuery)
  - GIST_LEAF (check if entry is leaf)
  - RTContainsStrategyNumber (contains strategy constant)
  - RTContainedByStrategyNumber (contained-by strategy constant)
- Called from (representative examples):
  - gtsquery_consistent_oldsig (backward compatibility version)

## Notes and Other Information
- This is a PostgreSQL extension function following PG_FUNCTION_ARGS convention
- Always sets *recheck = true because signature comparison is lossy
- Uses bitwise AND operations on TSQuerySign values for fast containment checks
- Different logic for leaf vs. internal nodes: exact matching for leaves, overlap checking for internals
- Supports PostgreSQL's text search containment operators (@@ and <@)
- Part of the TSQuery GiST operator class implementation providing efficient text search indexing