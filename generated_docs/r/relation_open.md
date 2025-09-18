# relation_open

## Location
src/backend/access/common/relation.c: 47 - 87

## Overview
Opens any relation by its object identifier (OID) and optionally acquires a lock on the relation. This is a fundamental function for accessing database relations in PostgreSQL.

## Definition


## Detailed Description
The `relation_open` function is the primary interface for opening database relations by their OID. It handles locking, relcache access, and validation to ensure safe access to database objects. The function performs several key operations:

1. **Lock Management**: If a lockmode other than NoLock is specified, it acquires the appropriate lock on the relation before opening it
2. **Relcache Access**: Uses the relation cache system to retrieve the relation descriptor
3. **Validation**: Ensures the relation exists and is accessible, raising an ERROR if not found
4. **Temporary Relation Tracking**: Marks when temporary relations are accessed for transaction management
5. **Statistics Initialization**: Initializes per-relation statistics tracking

The function is designed to work with any type of relation (tables, indexes, sequences, views, etc.) as defined in pg_class.

## Parameters / Member Variables
- `relationId`: The object identifier (OID) of the relation to open
- `lockmode`: The type of lock to acquire on the relation (NoLock means no lock acquisition)

## Dependencies
- Functions called/Symbols referenced:
  - LockRelationOid - Acquires lock on the relation
  - RelationIdGetRelation - Retrieves relation from relcache
  - RelationIsValid - Validates the relation descriptor
  - IsBootstrapProcessingMode - Checks if in bootstrap mode
  - CheckRelationLockedByMe - Verifies lock ownership
  - RelationUsesLocalBuffers - Checks if relation uses local buffers
  - pgstat_init_relation - Initializes relation statistics
  - MAX_LOCKMODES - Maximum lock mode constant
  - XACT_FLAGS_ACCESSEDTEMPNAMESPACE - Transaction flag for temp namespace access

- Called from (representative examples):
  - relation_openrv - Opens relation by name
  - relation_openrv_extended - Extended relation opening by name
  - index_open - Opens index relations
  - table_open - Opens table relations
  - sequence_open - Opens sequence relations
  - Various catalog and command functions

## Notes and Other Information
- The function raises an ERROR if the relation does not exist, so callers must be prepared to handle exceptions
- When NoLock is specified, the caller must already hold an appropriate lock (except in bootstrap mode)
- The function automatically tracks access to temporary relations for transaction management
- A "relation" in PostgreSQL context means any object with a pg_class entry, so callers should verify the relkind is appropriate for their use case
- The function integrates with PostgreSQL's statistics system by initializing per-relation stats tracking