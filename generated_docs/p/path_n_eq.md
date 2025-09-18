# path_n_eq

## Location
[src/backend/utils/adt/geo_ops.c:1571-1579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1571-L1579)

## Overview
Relational operator that compares two PATH objects for equality based on their cardinality (number of points), returning true if both paths have the same number of points.

## Definition


## Detailed Description
The `path_n_eq` function implements the "equal" operator for PATH data types based on path cardinality. It compares the number of points (`npts`) in two PATH structures and returns a boolean result indicating whether they have the same cardinality. This completes the set of basic relational operators for PATH types along with `path_n_lt` and `path_n_gt`.

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
- Uses simple cardinality comparison rather than geometric properties or coordinate values
- Completes the cardinality-based comparison trilogy with `path_n_lt` and `path_n_gt`
- Returns true if p1->npts == p2->npts, false otherwise
- Intended for use with PostgreSQL's operator system (likely the = operator for paths)
- Note that this only compares point count, not the actual path geometry or coordinates
- Subject to the same limitations as other path relational operators regarding simplistic comparison approach