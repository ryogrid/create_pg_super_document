# brin_inclusion_add_value

## Location
[src/backend/access/brin/brin_inclusion.c:138-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L138-L249)

## Overview
BRIN inclusion add value function that updates a BRIN index tuple by incorporating a new heap tuple value, expanding the inclusion set if necessary.

## Definition
```c
Datum brin_inclusion_add_value(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the core add_value handler for BRIN inclusion operator classes. It examines an existing index tuple representing a page range and determines if a new heap value needs to be incorporated into the inclusion summary. The function handles several key scenarios: initializing null ranges, detecting empty values, checking containment, testing mergeability, and performing union operations. When values cannot be merged, it marks the range as containing unmergeable elements. The function implements sophisticated logic to maintain minimal yet accurate inclusion summaries.

## Parameters / Member Variables  
- `bdesc` (BrinDesc *): BRIN index descriptor containing metadata and operator class information
- `column` (BrinValues *): Current BRIN values structure for the column being updated  
- `newval` (Datum): New value from heap tuple to potentially add to the inclusion summary
- `isnull` (bool): Flag indicating whether the new value is null (asserted to be false)
- `colloid` (Oid): Collation OID for comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - [inclusion_get_procinfo](../i/inclusion_get_procinfo.md)
  - [datumCopy](../d/datumCopy.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TupleDescAttr
  - [pfree](../p/pfree.md)
- Constants:
  - INCLUSION_UNION
  - INCLUSION_UNMERGEABLE  
  - INCLUSION_CONTAINS_EMPTY
  - PROCNUM_EMPTY
  - PROCNUM_CONTAINS
  - PROCNUM_MERGEABLE
  - PROCNUM_MERGE
- Data structures:
  - [BrinDesc](../B/BrinDesc.md)
  - [BrinValues](../B/BrinValues.md)
  - Form_pg_attribute
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - No direct references found (typically called via BRIN framework)

## Notes and Other Information
- Returns true if the index tuple was modified, false if no update was needed
- Handles three stored values per tuple: union, unmergeable flag, and contains-empty flag
- Implements lazy evaluation - stops processing early when possible (e.g., when unmergeable flag is set)
- Manages memory carefully by freeing old union values when they are replaced
- The function assumes the new value is not null (asserted with PG_USED_FOR_ASSERTS_ONLY)
- Contains optimization potential noted in comments regarding removal of union values when marked unmergeable

## Simplified Source

```c
Datum brin_inclusion_add_value(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
    Datum newval = PG_GETARG_DATUM(2);
    Oid colloid = PG_GET_COLLATION();

    AttrNumber attno = column->bv_attno;
    Form_pg_attribute attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
    bool new = false;

    // Initialize if this is the first non-null value
    if (column->bv_allnulls) {
        column->bv_values[INCLUSION_UNION] =
            datumCopy(newval, attr->attbyval, attr->attlen);
        column->bv_values[INCLUSION_UNMERGEABLE] = BoolGetDatum(false);
        column->bv_values[INCLUSION_CONTAINS_EMPTY] = BoolGetDatum(false);
        column->bv_allnulls = false;
        new = true;
    }

    // Skip processing if already marked as unmergeable
    if (DatumGetBool(column->bv_values[INCLUSION_UNMERGEABLE]))
        PG_RETURN_BOOL(false);

    // Check if new value is empty
    FmgrInfo *finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_EMPTY, true);
    if (finfo != NULL && DatumGetBool(FunctionCall1Coll(finfo, colloid, newval))) {
        if (!DatumGetBool(column->bv_values[INCLUSION_CONTAINS_EMPTY])) {
            column->bv_values[INCLUSION_CONTAINS_EMPTY] = BoolGetDatum(true);
            PG_RETURN_BOOL(true);
        }
        PG_RETURN_BOOL(false);
    }

    if (new)
        PG_RETURN_BOOL(true);

    // Check if new value is already contained
    finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_CONTAINS, true);
    if (finfo != NULL &&
        DatumGetBool(FunctionCall2Coll(finfo, colloid,
                                      column->bv_values[INCLUSION_UNION],
                                      newval)))
        PG_RETURN_BOOL(false);

    // Check if values are mergeable
    finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGEABLE, true);
    if (finfo != NULL &&
        !DatumGetBool(FunctionCall2Coll(finfo, colloid,
                                       column->bv_values[INCLUSION_UNION],
                                       newval))) {
        column->bv_values[INCLUSION_UNMERGEABLE] = BoolGetDatum(true);
        PG_RETURN_BOOL(true);
    }

    // Merge new value to existing union
    finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGE, false);
    Datum result = FunctionCall2Coll(finfo, colloid,
                                    column->bv_values[INCLUSION_UNION], newval);

    // Handle memory management for pass-by-reference types
    if (!attr->attbyval &&
        DatumGetPointer(result) != DatumGetPointer(column->bv_values[INCLUSION_UNION])) {
        pfree(DatumGetPointer(column->bv_values[INCLUSION_UNION]));
        if (result == newval)
            result = datumCopy(result, attr->attbyval, attr->attlen);
    }
    column->bv_values[INCLUSION_UNION] = result;

    PG_RETURN_BOOL(true);
}
```