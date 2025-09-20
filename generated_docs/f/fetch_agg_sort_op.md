# fetch_agg_sort_op

## Location
[src/backend/optimizer/plan/planagg.c:497-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L497-L512)

## Overview
Retrieves the sort operator OID associated with an aggregate function from the system catalog to determine if it's a MIN/MAX aggregate.

## Definition

```c
static Oid
fetch_agg_sort_op(Oid aggfnoid)
```
## Detailed Description
This function queries the PostgreSQL system catalog  to retrieve the sort operator (aggsortop) associated with a given aggregate function. The sort operator is a key indicator that distinguishes MIN/MAX aggregates from other aggregate functions:

1. **Catalog Lookup**: Uses  to fetch the aggregate's catalog entry by function OID
2. **Sort Operator Extraction**: Extracts the  field from the  row
3. **Validation**: Returns  if the aggregate function is not found in the catalog
4. **Resource Cleanup**: Releases the system cache reference

MIN and MAX aggregates have associated sort operators (< for MIN, > for MAX) that define the ordering used to determine the minimum or maximum value. Other aggregates like SUM, COUNT, AVG do not have sort operators and return , indicating they cannot be optimized using the MIN/MAX optimization strategy.

## Parameters / Member Variables
- : OID of the aggregate function to look up in the system catalog

## Dependencies
- Functions called/Symbols referenced:
  -  - Searches system catalog cache by aggregate function OID
  -  - Validates that the catalog lookup succeeded
  -  - Extracts the pg_aggregate structure from the heap tuple
  -  - Releases the system cache reference
  -  - Type definition for pg_aggregate catalog entries
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:281)

## Notes and Other Information
- Returns  for non-MIN/MAX aggregates or if the function OID is not found
- The  field contains the operator used for comparing values (e.g., int4lt for MIN on integers)
- This is a critical function for identifying which aggregates are eligible for index-based optimization
- The function assumes the aggregate function OID is valid and exists in the system
- Uses the PostgreSQL system cache for efficient repeated lookups