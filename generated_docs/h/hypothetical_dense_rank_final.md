# hypothetical_dense_rank_final

## Location
src/backend/utils/adt/orderedsetaggs.c: 1295 - 1430

## Overview
Implements the SQL dense rank function for hypothetical rows in ordered-set aggregates, calculating the dense rank (rank without gaps) of where a hypothetical row would appear in a dataset.

## Definition
```c
Datum hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the final phase of the dense_rank() ordered-set aggregate function for hypothetical rows. Unlike regular rank which can have gaps when there are ties, dense_rank provides consecutive ranking numbers by eliminating gaps. The function calculates where a hypothetical row would rank in an ordered dataset, but ensures that ranks are consecutive (1, 2, 3, ...) even when there are duplicate values.

The implementation differs significantly from the simpler percent_rank and cume_dist functions. It performs a complete sort of the data with the hypothetical row inserted, then iterates through all rows to count duplicates and calculate the proper dense rank. It uses tuple comparison logic to identify duplicate rows and subtract the duplicate count from the final rank to eliminate gaps.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the aggregate state and hypothetical row values
  - First argument contains the OSAPerGroupState with the dataset to be ranked
  - Subsequent arguments contain the hypothetical row values for ranking
- Local variables:
  - `rank`: Running count of position, starts at 1
  - `duplicate_count`: Number of duplicate values encountered before the hypothetical row
  - `econtext`: Expression context for tuple comparisons
  - `compareTuple`: Compiled expression for comparing tuple equality
  - `slot`, `slot2`, `extraslot`: Tuple table slots for managing row data during iteration

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext: Validates aggregate function call context
  - CreateStandaloneExprContext: Creates expression evaluation context
  - hypothetical_check_argtypes: Validates argument types for hypothetical functions
  - execTuplesMatchPrepare: Prepares tuple comparison expression
  - ExecClearTuple, ExecStoreVirtualTuple: Tuple slot manipulation functions
  - tuplesort_puttupleslot, tuplesort_performsort, tuplesort_gettupleslot: Tuple sorting operations
  - MakeSingleTupleTableSlot, ExecDropSingleTupleTableSlot: Tuple slot lifecycle management
  - ExecQualAndReset: Execute tuple comparison and reset expression state
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's aggregate function dispatch mechanism)

## Notes and Other Information
- This is the most complex of the hypothetical ranking functions due to the need to eliminate ranking gaps
- Uses a flag value of -1 when inserting the hypothetical row to sort it ahead of equal peers
- Performs tuple-by-tuple comparison using PostgreSQL's expression evaluation system to identify duplicates
- The algorithm alternates between two tuple slots to maintain access to the previous row for comparison
- Returns int64 values representing dense ranks (1, 2, 3, ... without gaps)
- Used in SQL queries like `SELECT dense_rank(value) WITHIN GROUP (ORDER BY column) FROM table`
- More computationally expensive than regular rank functions due to duplicate detection requirements
- Located in src/backend/utils/adt/orderedsetaggs.c:1295-1430