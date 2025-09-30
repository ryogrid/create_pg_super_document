# get_primary_key_attnos

## Location
[src/backend/catalog/pg_constraint.c:1149-1234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L1149-L1234)

## Overview
Identifies the columns that comprise a relation's primary key and returns them as a bitmap set of column attribute numbers.

## Definition
```c
Bitmapset *get_primary_key_attnos(Oid relid, bool deferrableOk, Oid *constraintOid)
```

## Detailed Description
This function searches the pg_constraint system catalog for the primary key constraint of a specified relation. When found, it extracts the column attribute numbers from the constraint's conkey array and returns them as a Bitmapset, with attribute numbers offset by FirstLowInvalidHeapAttributeNumber to allow representation of system columns.

The function specifically looks for constraints of type CONSTRAINT_PRIMARY and can optionally handle deferrable primary keys based on the deferrableOk parameter. Since there can be at most one primary key per table, the function stops searching once it finds a matching primary key constraint.

## Parameters / Member Variables
- `relid`: OID of the relation to search for a primary key
- `deferrableOk`: If false, ignores deferrable primary key constraints; if true, accepts both deferrable and immediate constraints
- `constraintOid`: Output parameter that receives the OID of the primary key constraint, or InvalidOid if not found

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - CONSTRAINT_PRIMARY
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [check_functional_grouping](../c/check_functional_grouping.md)
  - [remove_useless_groupby_columns](../r/remove_useless_groupby_columns.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- Returns NULL if no primary key exists or if a deferrable primary key is found but deferrableOk is false
- The function validates that the conkey array is properly formatted (1-D smallint array without nulls)
- Attribute numbers in the returned Bitmapset are offset by FirstLowInvalidHeapAttributeNumber to handle system columns
- Uses AccessShareLock on pg_constraint for consistent reads
- Early termination optimization: stops searching after finding the primary key since there can only be one per table
- Used in query optimization contexts where knowledge of primary key columns helps with grouping and uniqueness analysis

## Simplified Source

```c
Bitmapset *get_primary_key_attnos(Oid relid, bool deferrableOk, Oid *constraintOid) {
    Bitmapset *pkattnos = NULL;
    Relation pg_constraint;
    HeapTuple tuple;
    SysScanDesc scan;
    ScanKeyData skey[1];

    // Initialize output parameter
    *constraintOid = InvalidOid;

    // Open pg_constraint catalog and set up scan for this relation
    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(pg_constraint, ConstraintRelidTypidNameIndexId,
                              true, NULL, 1, skey);

    // Search for primary key constraint
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
        Datum adatum;
        bool isNull;
        ArrayType *arr;
        int16 *attnums;
        int numkeys;
        int i;

        // Only interested in primary key constraints
        if (con->contype != CONSTRAINT_PRIMARY)
            continue;

        // Check if deferrable constraints are acceptable
        if (con->condeferrable && !deferrableOk)
            break;

        // Extract the column attribute numbers from conkey array
        adatum = heap_getattr(tuple, Anum_pg_constraint_conkey,
                              RelationGetDescr(pg_constraint), &isNull);
        if (isNull)
            elog(ERROR, "null conkey for constraint %u", con->oid);

        arr = DatumGetArrayTypeP(adatum);
        numkeys = ARR_DIMS(arr)[0];

        // Validate array format
        if (ARR_NDIM(arr) != 1 || numkeys < 0 || ARR_HASNULL(arr) ||
            ARR_ELEMTYPE(arr) != INT2OID)
            elog(ERROR, "conkey is not a 1-D smallint array");

        attnums = (int16 *) ARR_DATA_PTR(arr);

        // Build bitmapset of primary key column numbers
        for (i = 0; i < numkeys; i++) {
            pkattnos = bms_add_member(pkattnos,
                                      attnums[i] - FirstLowInvalidHeapAttributeNumber);
        }

        *constraintOid = con->oid;
        break; // Only one primary key per table
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    return pkattnos;
}
```