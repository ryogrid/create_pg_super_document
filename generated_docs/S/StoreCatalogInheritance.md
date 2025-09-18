# StoreCatalogInheritance

## Location
[src/backend/commands/tablecmds.c:3389-3432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3389-L3432)

## Overview
StoreCatalogInheritance updates the PostgreSQL system catalogs with inheritance information for a newly created relation and its direct parent relations.

## Definition


## Detailed Description
This function is responsible for recording inheritance relationships in the PostgreSQL system catalog pg_inherits. It processes only direct ancestors (immediate parents) of a relation, creating entries in the inheritance catalog for each parent-child relationship. The function handles both regular table inheritance and table partitioning scenarios.

The function opens the pg_inherits system catalog with exclusive row lock and delegates the actual catalog entry creation to StoreCatalogInheritance1 for each parent relationship. It also ensures that parent relations are properly marked as having subclasses and establishes dependency relationships.

Historical note: Earlier versions of PostgreSQL maintained both direct and indirect ancestors in a pg_ipl catalog, but this is no longer necessary since that catalog was removed.

## Parameters / Member Variables
- : OID of the child relation that inherits from parent relations
- : List of OIDs representing the direct parent relations (ancestors)  
- : Boolean flag indicating whether the child is a partition (vs. regular inheritance)

## Dependencies
- Functions called/Symbols referenced:
  - [StoreCatalogInheritance1](StoreCatalogInheritance1.md)
  - table_open
  - table_close
  - lfirst_oid
  - Assert
  - OidIsValid
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)

## Notes and Other Information
- Only processes direct ancestors, not indirect ones (grandparents, etc.)
- Uses sequential numbering starting from 1 for inheritance ordering
- Performs sanity checks on the relation ID validity
- Returns early if no parent relations are provided (supers == NIL)
- Acquires RowExclusiveLock on pg_inherits catalog during operation
- Works for both regular table inheritance and table partitioning scenarios