# get_relation_constraint_oid

## Location
[src/backend/catalog/pg_constraint.c:897-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L897-L953)

## Overview
Searches for a constraint on a specified relation by name and returns its OID, with optional error handling for missing constraints.

## Definition
Oid get_relation_constraint_oid(Oid relid, const char *conname, bool missing_ok)

## Detailed Description
get_relation_constraint_oid performs a targeted lookup in the pg_constraint system catalog to find a constraint by name on a specific relation. The function uses an indexed scan with a three-part search key to efficiently locate the constraint:

1. **conrelid**: Matches the specified relation OID
2. **contypid**: Set to InvalidOid to search only relation constraints (not domain constraints)  
3. **conname**: Matches the specified constraint name

The function provides flexible error handling through the missing_ok parameter. When missing_ok is false, it reports a user-friendly error if the constraint doesn't exist. When true, it silently returns InvalidOid for missing constraints, allowing callers to handle the absence gracefully.

## Parameters / Member Variables
- : The OID of the relation to search for constraints on
- : The name of the constraint to locate (const char pointer)
- : Boolean flag controlling error behavior for missing constraints (true = return InvalidOid, false = raise error)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - HeapTupleIsValid
  - [systable_getnext](../s/systable_getnext.md)
  - GETSTRUCT
  - [systable_endscan](../s/systable_endscan.md)
  - OidIsValid
  - ereport
  - [get_rel_name](get_rel_name.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [get_object_address_relobject](get_object_address_relobject.md) (objectaddress.c:1460)
  - [rename_constraint_internal](../r/rename_constraint_internal.md) (tablecmds.c:3945)
  - [expandTableLikeClause](../e/expandTableLikeClause.md) (parse_utilcmd.c:1332)

## Notes and Other Information
- Uses ConstraintRelidTypidNameIndexId for efficient constraint lookup by composite key
- Employs AccessShareLock for read-only catalog access, allowing concurrent operations
- Guarantees at most one matching row due to unique constraint on the search key combination
- Provides user-friendly error messages including both constraint name and relation name
- Essential utility function for constraint name resolution in DDL operations and system utilities
- Only searches relation constraints by explicitly setting contypid to InvalidOid in search key

## Simplified Source

```c
Oid
get_relation_constraint_oid(Oid relid, const char *conname, bool missing_ok)
{
    Relation pg_constraint;
    HeapTuple tuple;
    SysScanDesc scan;
    ScanKeyData skey[3];
    Oid conOid = InvalidOid;

    // Open pg_constraint catalog for reading
    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    // Set up search keys for indexed scan
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(conname));

    // Perform indexed scan for matching constraint
    scan = systable_beginscan(pg_constraint, ConstraintRelidTypidNameIndexId, true,
                              NULL, 3, skey);

    // Extract OID if constraint found (at most one matching row)
    if (HeapTupleIsValid(tuple = systable_getnext(scan)))
        conOid = ((Form_pg_constraint) GETSTRUCT(tuple))->oid;

    systable_endscan(scan);

    // Report error if constraint not found and missing_ok is false
    if (!OidIsValid(conOid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("constraint \"%s\" for table \"%s\" does not exist",
                    conname, get_rel_name(relid))));

    table_close(pg_constraint, AccessShareLock);
    return conOid;
}
```