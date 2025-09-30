# findDomainNotNullConstraint

## Location
[src/backend/catalog/pg_constraint.c:569-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L569-L611)

## Overview
Finds and returns the pg_constraint tuple that implements a validated NOT NULL constraint for a given domain type.

## Definition

```c
HeapTuple
findDomainNotNullConstraint(Oid typid)
```
## Detailed Description
This function searches the pg_constraint catalog to locate a validated NOT NULL constraint associated with a specific domain type. It performs a sequential scan through all constraints belonging to the domain and returns the first validated NOT NULL constraint found. The function is specifically designed to work with domain types and their NOT NULL constraints, which are a special category of constraints in PostgreSQL's type system. It returns a copy of the constraint tuple to prevent issues with concurrent catalog modifications.

## Parameters / Member Variables
- : OID of the domain type to search for NOT NULL constraints

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
- Called from (representative examples):
  - [AlterDomainNotNull](../A/AlterDomainNotNull.md)

## Notes and Other Information
- Returns a HeapTuple (copy of the constraint tuple) if found, NULL otherwise
- Only returns validated NOT NULL constraints (convalidated = true)
- Uses ConstraintRelidTypidNameIndexId for efficient scanning
- Caller is responsible for freeing the returned HeapTuple
- Specific to domain type constraints, not table column constraints
- Part of the domain constraint management infrastructure

## Simplified Source

```c
HeapTuple
findDomainNotNullConstraint(Oid typid)
{
    Relation pg_constraint;
    HeapTuple conTup, retval = NULL;
    SysScanDesc scan;
    ScanKeyData key;

    // Open pg_constraint catalog for scanning
    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    // Set up scan key to find constraints for this domain type
    ScanKeyInit(&key, Anum_pg_constraint_contypid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(typid));

    // Begin scan using the constraint index
    scan = systable_beginscan(pg_constraint, ConstraintRelidTypidNameIndexId,
                              true, NULL, 1, &key);

    // Scan through all constraints for this domain
    while (HeapTupleIsValid(conTup = systable_getnext(scan)))
    {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);

        // Look for validated NOT NULL constraints only
        if (con->contype == CONSTRAINT_NOTNULL && con->convalidated)
        {
            retval = heap_copytuple(conTup);  // Return copy of found constraint
            break;
        }
    }

    // Clean up scan and close relation
    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    return retval;  // NULL if not found, tuple copy if found
}
```