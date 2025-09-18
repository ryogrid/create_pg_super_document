# hypothetical_rank_common

## Location
src/backend/utils/adt/orderedsetaggs.c: 1171 - 1243

## Overview
Core implementation function for hypothetical-set ranking aggregates that computes the rank of a hypothetical row within an ordered dataset.

## Definition
```c
static int64 hypothetical_rank_common(FunctionCallInfo fcinfo, int flag, int64 *number_of_rows)
```

## Detailed Description
This function implements the core logic for hypothetical-set ranking functions like `rank()`, `percent_rank()`, and `cume_dist()`. It takes a hypothetical row (specified by direct arguments) and determines what rank it would have if it were inserted into the aggregated dataset.

The function works by inserting the hypothetical row into the sorted dataset with a special flag value, completing the sort, then scanning through the results until it finds the hypothetical row. The rank is determined by counting how many rows come before the hypothetical row in the sorted order.

The flag parameter controls tie-breaking behavior: -1 sorts the hypothetical row ahead of its peers (for rank/dense_rank), while +1 sorts it behind (for percent_rank/cume_dist).

## Parameters / Member Variables
- `fcinfo`: Function call information containing both direct arguments (hypothetical row values) and aggregated state
- `flag`: Tie-breaking flag (-1 to sort ahead of peers, +1 to sort behind)
- `number_of_rows`: Output parameter returning the total count of regular (non-hypothetical) rows

## Dependencies
- Functions called/Symbols referenced:
  - `OSAPerGroupState`: Ordered-set aggregate per-group state structure
  - `AggCheckCallContext`: Validates aggregate calling context
  - `hypothetical_check_argtypes`: Validates argument type consistency
  - `ExecClearTuple`: Clears tuple slot contents
  - `ExecStoreVirtualTuple`: Stores virtual tuple in slot
  - `tuplesort_puttupleslot`: Adds tuple to sort operation
  - `tuplesort_performsort`: Completes the sorting operation
  - `tuplesort_gettupleslot`: Retrieves next tuple from sort
  - `slot_getattr`: Extracts attribute value from tuple slot
  - `DatumGetInt32`: Extracts int32 value from Datum
  - `CHECK_FOR_INTERRUPTS`: Allows query cancellation
- Called from (representative examples):
  - `hypothetical_rank_final`: Final function for rank() hypothetical-set aggregate
  - `hypothetical_percent_rank_final`: Final function for percent_rank() hypothetical-set aggregate
  - `hypothetical_cume_dist_final`: Final function for cume_dist() hypothetical-set aggregate

## Notes and Other Information
- This is a security-sensitive function that validates argument types before processing
- The function expects arguments in pairs: direct arguments followed by corresponding aggregated arguments
- Uses a special flag column to distinguish the hypothetical row from regular data rows
- Cannot share transition state with other aggregates due to the need to insert a hypothetical row
- The rank calculation is 1-based, consistent with SQL standard ranking functions
- Handles edge cases like empty datasets (returns rank 1) and null values appropriately
- The implementation is optimized for large datasets with interrupt checking during scanning