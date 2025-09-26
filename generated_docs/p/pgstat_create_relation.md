# pgstat_create_relation

## Location
[src/backend/utils/activity/pgstat_relation.c:169-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L169-L179)

## Overview
Registers a newly created relation with the statistics system in a transactional manner, ensuring that statistics entries are properly cleaned up if the creating transaction aborts.

## Definition
```c
void pgstat_create_relation(Relation rel)
```

## Detailed Description
This function is called when a new relation is created to ensure that its statistics entry is properly managed within the current transaction context. It leverages the transactional statistics framework to guarantee that if the transaction that created the relation is aborted, the corresponding statistics entry will be automatically cleaned up.

The function serves as a thin wrapper around `pgstat_create_transactional()`, providing the relation-specific parameters needed for transactional statistics management. It properly handles both shared and non-shared relations by determining the appropriate database ID context for the statistics entry.

This transactional approach prevents orphaned statistics entries that could occur if a relation creation transaction fails after the relation has been created but before the transaction commits.

## Parameters / Member Variables
- `rel`: The newly created Relation object that needs transactional statistics tracking

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_create_transactional
  - PGSTAT_KIND_RELATION (constant)
  - RelationGetRelid (macro)
  - InvalidOid (constant)
  - MyDatabaseId (global variable)
- Called from (representative examples):
  - heap_create (during table creation)

## Notes and Other Information
- This function is typically called during DDL operations that create new relations (tables, indexes, etc.)
- The transactional nature ensures automatic cleanup if the creating transaction aborts
- The function properly distinguishes between shared relations (system catalogs) and regular relations for database context
- For shared relations, InvalidOid is used as the database ID since they exist across all databases
- For regular relations, MyDatabaseId provides the appropriate database context
- This is part of the statistics system's integration with PostgreSQL's transaction management
- The function ensures that statistics tracking is established at relation creation time rather than at first access