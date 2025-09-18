# brin_minmax_consistent

## Location
src/backend/access/brin/brin_minmax.c: 137 - 207

## Overview
Determines whether a scan key is consistent with the min/max range stored in a BRIN index tuple, enabling the query planner to decide if a page range should be scanned.

## Definition
```c
Datum brin_minmax_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the consistency check for BRIN minmax indexes during query execution. It takes a scan key (representing a query condition) and compares it against the minimum and maximum values stored for a page range. Based on the comparison strategy, it determines whether the page range could potentially contain tuples matching the scan key.

The function handles different B-tree strategies:
- BTLessStrategyNumber/BTLessEqualStrategyNumber: Compares scan value against minimum
- BTEqualStrategyNumber: Checks if scan value falls within [min, max] range
- BTGreaterEqualStrategyNumber/BTGreaterStrategyNumber: Compares scan value against maximum

This enables efficient range pruning where entire page ranges can be skipped if they cannot possibly contain matching tuples.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN descriptor containing index metadata
- `column` (BrinValues *): Summary values (min/max) for the indexed column
- `key` (ScanKey): Scan key containing the query condition and strategy

## Dependencies
- Functions called/Symbols referenced:
  - BrinDesc (structure type)
  - BrinValues (structure type)
  - ScanKey (structure type)
  - minmax_get_strategy_procinfo (function to get comparison procedures)
  - FunctionCall2Coll (function call with collation)
  - PG_GET_COLLATION (macro to get collation)
  - PG_NARGS (macro to get argument count)
  - BTLessStrategyNumber, BTLessEqualStrategyNumber, BTEqualStrategyNumber
  - BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber (B-tree strategy constants)
  - PG_RETURN_DATUM (macro to return result)

- Called from (representative examples):
  - No direct callers found (likely called via function manager during index scans)

## Notes and Other Information
- Uses the old BRIN signature with only three arguments (asserted in the function)
- Should not receive all-NULL ranges as input (handled by AM code)
- For equality queries, performs two comparisons to ensure the value falls within the range
- Returns the result of the comparison operation directly as a Datum
- Strategy numbers correspond to standard B-tree operator strategies
- The function assumes NULL handling is done at a higher level in the access method code