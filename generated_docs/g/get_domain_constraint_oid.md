# get_domain_constraint_oid

## Location
[src/backend/catalog/pg_constraint.c:1090-1148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L1090-L1148)

## Overview
Finds a constraint on a specified domain by name and returns the constraint's OID.

## Definition
```c
Oid get_domain_constraint_oid(Oid typid, const char *conname, bool missing_ok)
```

## Detailed Description
This function searches the pg_constraint system catalog to find a constraint with the specified name on the given domain type. Unlike relation constraints, domain constraints are stored with a contypid value (the domain's type OID) and conrelid set to InvalidOid. The function performs a system catalog scan using three search keys to uniquely identify the domain constraint and returns its OID.

Domain constraints are check constraints that are applied to domain types, which are user-defined data types based on existing types with optional constraints and default values. This function enables lookups of these constraints by name within the context of a specific domain.

## Parameters / Member Variables
- `typid`: OID of the domain type to search for constraints
- `conname`: Name of the constraint to find
- `missing_ok`: If false, raises an error when the constraint is not found; if true, returns InvalidOid silently

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - [format_type_be](../f/format_type_be.md) (for error reporting)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - [rename_constraint_internal](../r/rename_constraint_internal.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- The function uses three scan keys: conrelid (set to InvalidOid for domain constraints), contypid (the domain's OID), and conname
- Domain constraints are typically CHECK constraints that validate values of the domain type
- Uses AccessShareLock on pg_constraint for consistent reads
- The function can handle missing constraints gracefully when missing_ok is true
- Error messages use format_type_be to provide a human-readable domain type name in error reports
- There can be at most one constraint with a given name on a specific domain

## Simplified Source

```c
Oid
get_domain_constraint_oid(Oid typid, const char *conname, bool missing_ok)
{
    Relation pg_constraint;
    HeapTuple tuple;
    SysScanDesc scan;
    ScanKeyData skey[3];
    Oid conOid = InvalidOid;

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    // Setup scan keys for domain constraint lookup
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(typid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(conname));

    scan = systable_beginscan(pg_constraint, ConstraintRelidTypidNameIndexId, true,
                              NULL, 3, skey);

    // Get constraint OID if found (at most one matching row)
    if (HeapTupleIsValid(tuple = systable_getnext(scan)))
        conOid = ((Form_pg_constraint) GETSTRUCT(tuple))->oid;

    systable_endscan(scan);

    // Handle missing constraint
    if (!OidIsValid(conOid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("constraint \"%s\" for domain %s does not exist",
                        conname, format_type_be(typid))));

    table_close(pg_constraint, AccessShareLock);

    return conOid;
}
```