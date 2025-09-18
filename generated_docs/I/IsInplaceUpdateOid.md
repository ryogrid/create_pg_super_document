# IsInplaceUpdateOid

## Location
[src/backend/catalog/catalog.c:162-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L162-L174)

## Overview
Determines whether a given relation OID identifies a table that receives in-place updates from PostgreSQL's core code.

## Definition


## Detailed Description
IsInplaceUpdateOid is a utility function that checks if a given relation OID corresponds to one of the core system tables that PostgreSQL modifies using in-place updates rather than the standard MVCC (Multi-Version Concurrency Control) mechanism. Currently, only two system tables receive in-place updates: pg_class (RelationRelationId = 1259) and pg_database (DatabaseRelationId = 1262).

In-place updates are used for these critical system tables to avoid the overhead of MVCC versioning for frequently updated metadata. This optimization is safe because these tables have controlled access patterns and specific locking protocols.

## Parameters
- : The OID of the relation to check

## Dependencies
- Functions called/Symbols referenced:
  - RelationRelationId (constant: 1259, pg_class table OID)
  - DatabaseRelationId (constant: 1262, pg_database table OID)
- Called from:
  - [IsInplaceUpdateRelation](IsInplaceUpdateRelation.md) (wrapper function that takes a Relation instead of OID)

## Notes and Other Information
- This function is used for assertions and to ensure the executor follows the proper locking protocol for in-place updated tables
- Extensions may perform in-place updates on other heap tables, but concurrent SQL UPDATE operations may overwrite those modifications
- The executor assumes that in-place updated relations are not partitions or partitioned tables and have no triggers
- Located in src/backend/catalog/catalog.c at lines 162-174