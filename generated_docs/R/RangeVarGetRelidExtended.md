# RangeVarGetRelidExtended

## Location
[src/backend/catalog/namespace.c:441-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L441-L653)

## Overview
A comprehensive function that resolves a RangeVar (relation name specification) to its actual OID, handling schema resolution, locking, and various error conditions with support for concurrent DDL operations.

## Definition


## Detailed Description
RangeVarGetRelidExtended is the core function for resolving relation names to OIDs in PostgreSQL. It performs a sophisticated name lookup that handles schema qualification, temporary table resolution, and concurrent DDL safety through an invalidation message retry mechanism.

The function operates in a retry loop to handle concurrent DDL operations that might change the relation being looked up. It supports various behavioral flags for missing relations, lock waiting policies, and includes a callback mechanism for permission checks and additional processing.

Key features include:
- Cross-database reference validation (with appropriate error reporting)
- Special handling for temporary tables (RELPERSISTENCE_TEMP)  
- Schema-qualified and unqualified name resolution
- Invalidation message processing to handle concurrent DDL
- Flexible locking policies with NOWAIT and SKIP_LOCKED options
- Callback mechanism for custom validation logic

## Parameters / Member Variables
- : RangeVar structure containing the relation name, optional schema name, and persistence information
- : Type of lock to acquire on the relation (or NoLock to skip locking)
- : Bitmask controlling behavior (RVR_MISSING_OK, RVR_NOWAIT, RVR_SKIP_LOCKED)
- : Optional function called after name resolution but before locking for custom validation
- : Argument passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [get_database_name](../g/get_database_name.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)  
  - [get_relname_relid](../g/get_relname_relid.md)
  - [RelnameGetRelid](RelnameGetRelid.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
- Called from (representative examples):
  - RangeVarGetRelid (inline wrapper)
  - [cluster](../c/cluster.md)
  - [LockTableCommand](../L/LockTableCommand.md)
  - [RemoveRelations](RemoveRelations.md)
  - [ExecuteTruncate](../E/ExecuteTruncate.md)

## Notes and Other Information
- Returns InvalidOid when relation is not found and RVR_MISSING_OK flag is set
- Implements sophisticated retry logic using SharedInvalidMessageCounter to handle concurrent DDL
- Special logic for temporary tables ensures they are found even when pg_temp is not first in search path
- Flags RVR_NOWAIT and RVR_SKIP_LOCKED are mutually exclusive
- The callback mechanism allows callers to perform permission checks before the relation is locked