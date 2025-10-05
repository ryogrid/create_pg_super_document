# brin_inclusion_union

## Location
[src/backend/access/brin/brin_inclusion.c:474-543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L474-L543)

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
  - [inclusion_get_procinfo](../i/inclusion_get_procinfo.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TupleDescAttr
  - [pfree](../p/pfree.md)
  - [datumCopy](../d/datumCopy.md)
  - PG_GET_COLLATION
  - PG_RETURN_VOID
- Constants:
  - INCLUSION_CONTAINS_EMPTY
  - INCLUSION_UNMERGEABLE
  - INCLUSION_UNION
  - PROCNUM_MERGEABLE
  - PROCNUM_MERGE
- Data structures:
  - [BrinDesc](../B/BrinDesc.md)
  - [BrinValues](../B/BrinValues.md)
  - Form_pg_attribute
  - [FmgrInfo](../F/FmgrInfo.md)
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

## Simplified Source

```c
Datum brin_inclusion_union(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
    BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
    BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
    Oid colloid = PG_GET_COLLATION();

    AttrNumber attno = col_a->bv_attno;
    Form_pg_attribute attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

    // Propagate contains-empty flag from B to A if needed
    if (!DatumGetBool(col_a->bv_values[INCLUSION_CONTAINS_EMPTY]) &&
        DatumGetBool(col_b->bv_values[INCLUSION_CONTAINS_EMPTY]))
        col_a->bv_values[INCLUSION_CONTAINS_EMPTY] = BoolGetDatum(true);

    // Early exit if A already has unmergeable elements
    if (DatumGetBool(col_a->bv_values[INCLUSION_UNMERGEABLE]))
        PG_RETURN_VOID();

    // Propagate unmergeable flag from B to A if needed
    if (DatumGetBool(col_b->bv_values[INCLUSION_UNMERGEABLE])) {
        col_a->bv_values[INCLUSION_UNMERGEABLE] = BoolGetDatum(true);
        PG_RETURN_VOID();
    }

    // Check if A and B union values are mergeable
    FmgrInfo *finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGEABLE, true);
    if (finfo != NULL &&
        !DatumGetBool(FunctionCall2Coll(finfo, colloid,
                                       col_a->bv_values[INCLUSION_UNION],
                                       col_b->bv_values[INCLUSION_UNION]))) {
        col_a->bv_values[INCLUSION_UNMERGEABLE] = BoolGetDatum(true);
        PG_RETURN_VOID();
    }

    // Merge B's union value into A's union
    finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGE, false);
    Datum result = FunctionCall2Coll(finfo, colloid,
                                    col_a->bv_values[INCLUSION_UNION],
                                    col_b->bv_values[INCLUSION_UNION]);

    // Handle memory management for pass-by-reference types
    if (!attr->attbyval &&
        DatumGetPointer(result) != DatumGetPointer(col_a->bv_values[INCLUSION_UNION])) {
        pfree(DatumGetPointer(col_a->bv_values[INCLUSION_UNION]));
        if (result == col_b->bv_values[INCLUSION_UNION])
            result = datumCopy(result, attr->attbyval, attr->attlen);
    }
    col_a->bv_values[INCLUSION_UNION] = result;

    PG_RETURN_VOID();
}
```