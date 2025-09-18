# get_typavgwidth

## Location
src/backend/utils/cache/lsyscache.c: 2578 - 2628

## Overview
Estimates the average width (in bytes) of values for a given PostgreSQL data type, used by the query planner for cost estimation and memory allocation planning.

## Definition


## Detailed Description
This function provides width estimates for PostgreSQL data types to help the query planner make informed decisions about memory usage, disk I/O costs, and execution strategies. The function handles different categories of types with varying levels of precision:

1. **Fixed-width types**: Returns the exact type length from the system catalog
2. **Variable-width types with known maximum**: Uses heuristics based on the maximum possible size
3. **Completely unknown types**: Falls back to a conservative default estimate

For variable-width types, the function employs a sliding scale approach:
- Types with max width ≤ 32 bytes: assumes full utilization
- Types with max width < 1000 bytes: assumes 50% utilization beyond the first 32 bytes
- Types with max width ≥ 1000 bytes: uses a fixed estimate assuming the declared limit is rarely reached

Special handling exists for BPCHAR (blank-padded character) types, which always use their full declared width.

## Parameters / Member Variables
- : OID of the data type to estimate
- : Type modifier value (pass -1 if unknown); affects maximum size calculations for certain types

## Dependencies
- Functions called/Symbols referenced:
  - get_typlen (retrieve fixed type length)
  - type_maximum_size (calculate maximum possible size given typmod)
  - BPCHAROID (constant for blank-padded character type)

- Called from (representative examples):
  - set_rel_width (src/backend/optimizer/path/costsize.c:6171, 6201)
  - get_expr_width (src/backend/optimizer/path/costsize.c:6328, 6334)
  - set_append_rel_size (src/backend/optimizer/path/allpaths.c:1182)
  - create_one_window_path (src/backend/optimizer/plan/planner.c:4752)
  - get_rel_data_width (src/backend/optimizer/util/plancat.c:1209)

## Notes and Other Information
- The function is designed for planner use and doesn't require absolute accuracy; reasonable approximations are acceptable
- The default fallback estimate of 32 bytes represents a conservative guess for unknown types
- Critical for memory management in operations like sorting, hashing, and temporary file creation
- The heuristics are based on empirical observations of typical data distribution patterns in PostgreSQL databases
- Located in lsyscache.c as part of the system catalog caching infrastructure