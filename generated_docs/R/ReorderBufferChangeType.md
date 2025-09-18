# ReorderBufferChangeType

## Location
[src/include/replication/reorderbuffer.h:59-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/reorderbuffer.h#L59-L70)

## Overview
An enumeration that defines the types of changes that can be recorded in PostgreSQL's reorder buffer during logical replication and WAL decoding operations.

## Definition


## Detailed Description
This enumeration categorizes different types of changes that occur during database operations and are tracked by PostgreSQL's reorder buffer system. The reorder buffer is a critical component of logical replication that collects and orders changes from the WAL (Write-Ahead Log) for delivery to logical replication consumers.

The enum serves two main purposes:
1. **User-visible changes**: INSERT, UPDATE, DELETE, MESSAGE, INVALIDATION, and TRUNCATE operations that are exposed to logical decoding output plugins
2. **Internal changes**: Various internal housekeeping operations (prefixed with INTERNAL_) that are used internally but never exposed to users of the decoding facilities

For efficiency and simplicity, the reorder buffer stores both user-visible changes and internal metadata (snapshots, command IDs, combo CIDs) in the same data structure, differentiated by this enum type.

## Parameters / Member Variables
- : Represents a row insertion operation
- : Represents a row update operation  
- : Represents a row deletion operation
- : Represents a logical replication message
- : Represents a cache invalidation event
- : Internal snapshot management (not user-visible)
- : Internal command ID tracking (not user-visible)
- : Internal tuple command ID tracking (not user-visible)
- : Internal speculative insertion for INSERT..ON CONFLICT (not user-visible)
- : Internal confirmation of speculative insertion (not user-visible)
- : Internal abort of speculative insertion (not user-visible)
- : Represents a table truncation operation

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no direct function calls)
- Called from (representative examples):
  -  (src/backend/replication/pgoutput/pgoutput.c:1250)
  -  (src/backend/replication/pgoutput/pgoutput.c:1439)
  -  struct (src/include/replication/reorderbuffer.h:76)

## Notes and Other Information
- The INTERNAL_SPEC_* values are specifically related to PostgreSQL's INSERT..ON CONFLICT..UPDATE feature and handle speculative insertions that may be confirmed or aborted
- Users of logical decoding APIs will never encounter changes with INTERNAL_* action types as these are filtered out before being passed to output plugins
- This enum is fundamental to PostgreSQL's logical replication architecture and is used throughout the reorder buffer subsystem
- The enum is defined in src/include/replication/reorderbuffer.h:45-59