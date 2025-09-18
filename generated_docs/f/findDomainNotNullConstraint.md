# findDomainNotNullConstraint

## Location
[src/backend/catalog/pg_constraint.c:569-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L569-L611)

## Overview
Finds and returns the pg_constraint tuple that implements a validated NOT NULL constraint for a given domain type.

## Definition


## Detailed Description
This function searches the pg_constraint catalog to locate a validated NOT NULL constraint associated with a specific domain type. It performs a sequential scan through all constraints belonging to the domain and returns the first validated NOT NULL constraint found. The function is specifically designed to work with domain types and their NOT NULL constraints, which are a special category of constraints in PostgreSQL's type system. It returns a copy of the constraint tuple to prevent issues with concurrent catalog modifications.

## Parameters / Member Variables
- : OID of the domain type to search for NOT NULL constraints

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
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