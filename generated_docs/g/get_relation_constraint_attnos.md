# get_relation_constraint_attnos

## Location
[src/backend/catalog/pg_constraint.c:954-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L954-L1042)

## Overview
Finds a constraint on a specified relation by name and returns the constrained columns as a bitmap set of column attribute numbers.

## Definition
```c
Bitmapset *get_relation_constraint_attnos(Oid relid, const char *conname, bool missing_ok, Oid *constraintOid)
```

## Detailed Description
This function searches the pg_constraint system catalog to find a constraint with the specified name on the given relation. It returns the attribute numbers of the constrained columns as a Bitmapset, with attribute numbers offset by FirstLowInvalidHeapAttributeNumber to allow representation of system columns. The function also returns the OID of the matching constraint through an output parameter.

The function performs a system catalog scan using three search keys: relation OID, constraint type (set to InvalidOid to match relation constraints), and constraint name. When a matching constraint is found, it extracts the conkey array which contains the attribute numbers of the constrained columns and converts them into a Bitmapset.

## Parameters / Member Variables
- `relid`: OID of the relation to search for constraints
- `conname`: Name of the constraint to find
- `missing_ok`: If false, raises an error when the constraint is not found; if true, returns NULL silently
- `constraintOid`: Output parameter that receives the OID of the found constraint, or InvalidOid if not found

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
  - [get_rel_name](get_rel_name.md)
- Called from (representative examples):
  - [transformOnConflictArbiter](../t/transformOnConflictArbiter.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- The function accesses pg_constraint with AccessShareLock to ensure consistent reads
- Attribute numbers in the returned Bitmapset are offset by FirstLowInvalidHeapAttributeNumber to handle system columns
- The function validates that the conkey array is properly formatted (1-D smallint array without nulls)
- Only relation constraints are matched (contypid is set to InvalidOid in the search key)
- The function can handle missing constraints gracefully when missing_ok is true

## Simplified Source

```c
Bitmapset *
get_relation_constraint_attnos(Oid relid, const char *conname,
                             bool missing_ok, Oid *constraintOid)
{
    Bitmapset *conattnos = NULL;
    Relation pg_constraint;
    HeapTuple tuple;
    SysScanDesc scan;
    ScanKeyData skey[3];

    *constraintOid = InvalidOid;

    // Open pg_constraint catalog
    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    // Set up scan keys: relation OID, type (InvalidOid for relation constraints), name
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(conname));

    scan = systable_beginscan(pg_constraint, ConstraintRelidTypidNameIndexId,
                             true, NULL, 3, skey);

    // Search for matching constraint
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum adatum;
        bool isNull;

        *constraintOid = ((Form_pg_constraint) GETSTRUCT(tuple))->oid;

        // Extract constrained column numbers from conkey array
        adatum = heap_getattr(tuple, Anum_pg_constraint_conkey,
                             RelationGetDescr(pg_constraint), &isNull);
        if (!isNull) {
            ArrayType *arr = DatumGetArrayTypeP(adatum);
            int numcols = ARR_DIMS(arr)[0];
            int16 *attnums;

            // Validate array format
            if (ARR_NDIM(arr) != 1 || numcols < 0 ||
                ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID)
                elog(ERROR, "conkey is not a 1-D smallint array");

            attnums = (int16 *) ARR_DATA_PTR(arr);

            // Build bitmap of attribute numbers
            for (int i = 0; i < numcols; i++) {
                conattnos = bms_add_member(conattnos,
                    attnums[i] - FirstLowInvalidHeapAttributeNumber);
            }
        }
    }

    systable_endscan(scan);

    // Handle missing constraint
    if (!OidIsValid(*constraintOid) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("constraint \"%s\" for table \"%s\" does not exist",
                   conname, get_rel_name(relid))));

    table_close(pg_constraint, AccessShareLock);
    return conattnos;
}
```