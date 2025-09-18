# path_n_gt

## Location
src/backend/utils/adt/geo_ops.c: 1562 - 1570

## Overview
Relational operator that compares two PATH objects based on their cardinality (number of points), returning true if the first path has more points than the second.

## Definition


## Detailed Description
The `path_n_gt` function implements the "greater than" operator for PATH data types based on path cardinality. It compares the number of points (`npts`) in two PATH structures and returns a boolean result. This is the complement to `path_n_lt` and follows the same simple cardinality-based comparison approach.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `p1`: First PATH pointer to compare
  - `p2`: Second PATH pointer to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (extracts PATH pointers from function arguments)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found (likely referenced through operator framework)

## Notes and Other Information
- Part of PostgreSQL's relational operators for PATH geometric type
- Uses simple cardinality comparison rather than geometric properties
- Companion function to `path_n_lt` and `path_n_eq`
- Returns true if p1->npts > p2->npts, false otherwise
- Intended for use with PostgreSQL's operator system (likely the > operator for paths)
- Subject to the same limitations mentioned in the source comments about being a basic implementation