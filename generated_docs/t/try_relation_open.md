# try_relation_open

## Location
src/backend/access/common/relation.c: 88 - 136

## Overview
Opens a relation by its OID with the same functionality as relation_open, but returns NULL instead of raising an error if the relation does not exist.

## Definition


## Detailed Description
The `try_relation_open` function provides a non-failing alternative to `relation_open`. It performs the same core operations but uses a defensive approach that allows callers to handle non-existent relations gracefully. The function includes these key steps:

1. **Lock Acquisition**: Acquires the specified lock on the relation if lockmode is not NoLock
2. **Existence Check**: Uses the system catalog cache to verify the relation exists before attempting relcache access
3. **Graceful Failure**: If the relation doesn't exist, releases any acquired lock and returns NULL
4. **Safe Relcache Access**: Only attempts to load from relcache after confirming existence
5. **Validation and Setup**: Performs the same validation, temporary relation tracking, and statistics initialization as relation_open

This function is particularly useful in scenarios where relation existence is uncertain and the caller wants to handle missing relations without exception handling.

## Parameters / Member Variables
- `relationId`: The object identifier (OID) of the relation to open
- `lockmode`: The type of lock to acquire on the relation (NoLock means no lock acquisition)

## Dependencies
- Functions called/Symbols referenced:
  - LockRelationOid - Acquires lock on the relation
  - SearchSysCacheExists1 - Checks if relation exists in system catalog
  - UnlockRelationOid - Releases lock if relation doesn't exist
  - RelationIdGetRelation - Retrieves relation from relcache
  - RelationIsValid - Validates the relation descriptor
  - CheckRelationLockedByMe - Verifies lock ownership
  - RelationUsesLocalBuffers - Checks if relation uses local buffers
  - pgstat_init_relation - Initializes relation statistics
  - MAX_LOCKMODES - Maximum lock mode constant
  - XACT_FLAGS_ACCESSEDTEMPNAMESPACE - Transaction flag for temp namespace access

- Called from (representative examples):
  - try_index_open - Non-failing index opening
  - try_table_open - Non-failing table opening
  - cluster_rel - Cluster operation
  - ATExecDetachPartition - Partition detachment
  - vacuum_open_relation - Vacuum operations
  - pg_relation_size - Size calculation functions

## Notes and Other Information
- Returns NULL if the relation does not exist, unlike relation_open which raises an ERROR
- Performs existence check using system catalog cache before relcache access for efficiency
- Automatically releases any acquired lock if the relation is found not to exist
- Still raises an ERROR if relcache loading fails for an existing relation (indicating a system problem)
- Useful for operations that need to handle potentially dropped or non-existent relations gracefully
- The existence check helps avoid unnecessary relcache invalidation messages and potential race conditions