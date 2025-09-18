# brin_inclusion_union

## Location
src/backend/access/brin/brin_inclusion.c: 474 - 543

## Overview
BRIN inclusion union function that merges two BRIN value summaries by updating the first to represent the union of both ranges.

## Definition
```c
Datum brin_inclusion_union(PG_FUNCTION_ARGS)
```

## Detailed Description  
This function implements the union operation for BRIN inclusion operator classes, combining two BrinValues structures representing different page ranges into a single summary. The function modifies the first BrinValues (col_a) to contain the union of both summaries while leaving the second (col_b) unchanged. It handles the propagation of special flags (contains-empty and unmergeable), tests mergeability between the union values, and performs the actual merge operation. The function includes careful memory management to avoid leaks when replacing union values, and implements early termination when unmergeable conditions are detected.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN index descriptor containing metadata and operator class information
- `col_a` (BrinValues *): First BRIN values structure to be updated with the union result  
- `col_b` (BrinValues *): Second BRIN values structure to merge from (remains unchanged)
- `colloid` (Oid): Collation OID for comparison and merge operations

## Dependencies
- Functions called/Symbols referenced:
  - inclusion_get_procinfo
  - FunctionCall2Coll
  - DatumGetBool
  - BoolGetDatum
  - DatumGetPointer
  - TupleDescAttr
  - pfree
  - datumCopy
  - PG_GET_COLLATION
  - PG_RETURN_VOID
- Constants:
  - INCLUSION_CONTAINS_EMPTY
  - INCLUSION_UNMERGEABLE
  - INCLUSION_UNION
  - PROCNUM_MERGEABLE
  - PROCNUM_MERGE
- Data structures:
  - BrinDesc
  - BrinValues
  - Form_pg_attribute
  - FmgrInfo
- Called from (representative examples):
  - No direct references found (typically called via BRIN framework during summarization)

## Notes and Other Information
- Asserts that both BrinValues have the same attribute number and are non-null
- Propagates the contains-empty flag from col_b to col_a if needed
- Short-circuits if col_a already contains unmergeable elements
- Marks col_a as unmergeable if col_b contains unmergeable elements or if the union values are not mergeable
- Performs actual merge operation only after all mergeability checks pass
- Handles memory management carefully by freeing old union values when they are replaced
- Copies the result datum if it points to col_b's data to avoid aliasing issues
- Returns void as it modifies col_a in-place rather than returning a new structure