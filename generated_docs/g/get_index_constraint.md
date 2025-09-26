# get_index_constraint

## Location
[src/backend/catalog/pg_depend.c:989-1044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L989-L1044)

## Overview
Retrieves the OID of the constraint (unique, primary key, or exclusion) that owns a given index, returning InvalidOid if no owning constraint exists.

## Definition
Oid get_index_constraint(Oid indexId)

## Detailed Description
This function searches the PostgreSQL dependency system to find the constraint that owns a specific index. It scans the pg_depend system catalog to locate internal dependencies between the index and any constraint objects. The function specifically looks for constraints of types unique, primary key, or exclusion that have an internal dependency relationship with the given index. This is essential for understanding the relationship between indexes and their associated constraints in PostgreSQL's constraint management system.

## Parameters / Member Variables
- `indexId`: The OID of the index for which to find the owning constraint

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_INTERNAL
- Called from (representative examples):
  - [index_concurrently_swap](../i/index_concurrently_swap.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [RememberIndexForRebuilding](../R/RememberIndexForRebuilding.md)
  - [generateClonedIndexStmt](generateClonedIndexStmt.md)
  - [transformIndexConstraint](../t/transformIndexConstraint.md)

## Notes and Other Information
The function performs a catalog scan on pg_depend using the DependDependerIndexId index for efficient lookup. It specifically searches for internal dependencies where the index is the dependent object and a constraint is the referenced object. This relationship is crucial for PostgreSQL's constraint system, as constraints like primary keys and unique constraints are implemented using indexes. The function returns InvalidOid when no constraint owns the index, which is the case for indexes created independently of constraints.