# DeconstructFkConstraintRow

## Location
[src/backend/catalog/pg_constraint.c:1235-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L1235-L1366)

## Overview
Extracts foreign key constraint data from a pg_constraint tuple into separate arrays for constraint columns, referenced columns, and operator information.

## Definition
```c
void DeconstructFkConstraintRow(HeapTuple tuple, int *numfks,
                               AttrNumber *conkey, AttrNumber *confkey,
                               Oid *pf_eq_oprs, Oid *pp_eq_oprs, Oid *ff_eq_oprs,
                               int *num_fk_del_set_cols, AttrNumber *fk_del_set_cols)
```

## Detailed Description
This function parses a foreign key constraint tuple from pg_constraint and extracts all relevant information into caller-provided arrays. It processes the constraint's stored arrays (conkey, confkey, operator arrays, and optional delete set columns) and validates their structure before copying the data to output parameters.

The function handles the complex structure of foreign key constraints, which include not only the referencing and referenced column lists, but also the equality operators used for different types of comparisons (primary-foreign, primary-primary, foreign-foreign) and optional columns for SET operations during deletion.

All array fields are validated to ensure they are properly formatted 1-dimensional arrays without nulls and with the expected element types. The function also handles detoasting of arrays and proper memory management.

## Parameters / Member Variables
- `tuple`: HeapTuple from pg_constraint containing the foreign key constraint data
- `numfks`: Output parameter receiving the number of foreign key columns
- `conkey`: Output array receiving the attribute numbers of the referencing columns
- `confkey`: Output array receiving the attribute numbers of the referenced columns  
- `pf_eq_oprs`: Output array receiving primary-foreign equality operator OIDs (optional, can be NULL)
- `pp_eq_oprs`: Output array receiving primary-primary equality operator OIDs (optional, can be NULL)
- `ff_eq_oprs`: Output array receiving foreign-foreign equality operator OIDs (optional, can be NULL)
- `num_fk_del_set_cols`: Output parameter receiving the number of SET columns for deletion (optional, can be NULL)
- `fk_del_set_cols`: Output array receiving SET column attribute numbers for deletion (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetArrayTypeP
  - ARR_NDIM, ARR_DIMS, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - INDEX_MAX_KEYS
  - Pointer
  - memcpy
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CloneFkReferenced](../C/CloneFkReferenced.md)
  - [CloneFkReferencing](../C/CloneFkReferencing.md)
  - [DetachPartitionFinalize](DetachPartitionFinalize.md)
  - [ri_LoadConstraintInfo](../r/ri_LoadConstraintInfo.md)
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- All output arguments except numfks, conkey, and confkey can be passed as NULL if the caller doesn't need them
- The function enforces that foreign key constraints cannot have more than INDEX_MAX_KEYS columns
- Proper memory management is handled for detoasted arrays
- The confdelsetcols array (for SET operations) is optional and may be NULL in the constraint tuple
- Array validation ensures all arrays are 1-dimensional, properly typed, and contain no NULL elements
- Used extensively in foreign key constraint processing, cloning, and referential integrity trigger setup

## Simplified Source

```c
void DeconstructFkConstraintRow(HeapTuple tuple, int *numfks,
                               AttrNumber *conkey, AttrNumber *confkey,
                               Oid *pf_eq_oprs, Oid *pp_eq_oprs, Oid *ff_eq_oprs,
                               int *num_fk_del_set_cols, AttrNumber *fk_del_set_cols) {
    Datum adatum;
    bool isNull;
    ArrayType *arr;
    int numkeys;

    // Extract conkey array (referencing columns)
    adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conkey);
    arr = DatumGetArrayTypeP(adatum);

    // Validate array structure
    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID)
        elog(ERROR, "conkey is not a 1-D smallint array");

    numkeys = ARR_DIMS(arr)[0];
    if (numkeys <= 0 || numkeys > INDEX_MAX_KEYS)
        elog(ERROR, "foreign key constraint cannot have %d columns", numkeys);

    // Copy referencing column numbers
    memcpy(conkey, ARR_DATA_PTR(arr), numkeys * sizeof(int16));
    if ((Pointer) arr != DatumGetPointer(adatum))
        pfree(arr);

    // Extract confkey array (referenced columns)
    adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_confkey);
    arr = DatumGetArrayTypeP(adatum);

    // Validate and copy referenced column numbers
    if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numkeys ||
        ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID)
        elog(ERROR, "confkey is not a 1-D smallint array");

    memcpy(confkey, ARR_DATA_PTR(arr), numkeys * sizeof(int16));
    if ((Pointer) arr != DatumGetPointer(adatum))
        pfree(arr);

    // Extract operator arrays if requested
    if (pf_eq_oprs) {
        adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conpfeqop);
        arr = DatumGetArrayTypeP(adatum);

        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numkeys ||
            ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID)
            elog(ERROR, "conpfeqop is not a 1-D Oid array");

        memcpy(pf_eq_oprs, ARR_DATA_PTR(arr), numkeys * sizeof(Oid));
        if ((Pointer) arr != DatumGetPointer(adatum))
            pfree(arr);
    }

    // Similar extraction for pp_eq_oprs and ff_eq_oprs if requested
    // (code structure same as pf_eq_oprs)

    // Extract delete SET columns if requested
    if (fk_del_set_cols) {
        adatum = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_confdelsetcols, &isNull);

        if (isNull) {
            *num_fk_del_set_cols = 0;
        } else {
            arr = DatumGetArrayTypeP(adatum);

            if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID)
                elog(ERROR, "confdelsetcols is not a 1-D smallint array");

            int num_delete_cols = ARR_DIMS(arr)[0];
            memcpy(fk_del_set_cols, ARR_DATA_PTR(arr), num_delete_cols * sizeof(int16));

            if ((Pointer) arr != DatumGetPointer(adatum))
                pfree(arr);

            *num_fk_del_set_cols = num_delete_cols;
        }
    }

    *numfks = numkeys;
}
```