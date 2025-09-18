# brin_minmax_multi_consistent

## Location
src/backend/access/brin/brin_minmax_multi.c: 2549 - 2734

## Overview
Determines whether scan keys are consistent with BRIN minmax multi-column index summaries by evaluating query conditions against stored range and discrete value summaries.

## Definition
```c
Datum brin_minmax_multi_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the core consistency checking mechanism for BRIN minmax multi-column indexes during query execution. It evaluates whether a given page range might contain tuples that satisfy the scan keys by examining the index's stored min/max ranges and discrete values.

The function operates in two main phases:

1. **Range Evaluation**: Examines stored min/max ranges to determine if any range could potentially contain matching values. For each range, it tests all scan keys using appropriate comparison operators:
   - For less-than operations: Tests against the minimum value in the range
   - For greater-than operations: Tests against the maximum value in the range  
   - For equality operations: Performs boundary checks to see if the value could fall within the range

2. **Discrete Value Evaluation**: If no ranges match, examines individual stored values that didn't fit into ranges, testing each value directly against all scan keys using the appropriate comparison operators.

The function deserializes the stored range summary data and systematically evaluates each scan key against the summary information. It returns true if any range or discrete value could potentially satisfy all scan keys, indicating the page range should be scanned.

## Parameters / Member Variables
- `bdesc`: BRIN descriptor containing index metadata and operator information
- `column`: BrinValues structure containing the serialized range summary data
- `keys`: Array of ScanKey structures representing the query conditions to evaluate
- `nkeys`: Number of scan keys in the keys array

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER/PG_GETARG_INT32: Extract function arguments
  - PG_GET_COLLATION: Get collation for comparisons
  - PG_DETOAST_DATUM: Deserialize toasted datum
  - [brin_range_deserialize](brin_range_deserialize.md): Convert serialized ranges to memory format
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md): Get comparison function information
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md): Execute comparison functions with collation
  - [DatumGetBool](../D/DatumGetBool.md): Extract boolean result from datum
- Called from (representative examples):
  - Not directly referenced by other symbols (likely called through function pointer mechanism during index scans)

## Notes and Other Information
- Returns boolean indicating whether the page range should be included in the scan
- Handles all B-tree strategy numbers: less-than, less-equal, equal, greater-equal, greater-than
- Uses short-circuit evaluation - returns true immediately upon finding a matching range or value
- Includes assertions to ensure NULL keys are filtered out before processing
- The equality strategy requires special handling with boundary comparisons
- Part of the BRIN query execution pipeline for efficient page-range filtering
- Critical for index scan performance - false positives are acceptable but false negatives would cause incorrect query results
- Operates on serialized/compacted data for optimal storage and processing efficiency