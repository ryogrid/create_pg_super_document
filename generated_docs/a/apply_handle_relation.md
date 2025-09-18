# apply_handle_relation

## Location
src/backend/replication/logical/worker.c: 2303 - 2325

## Overview
apply_handle_relation processes RELATION messages in PostgreSQL logical replication, updating the local relation mapping and partition information without immediate schema validation.

## Definition
```c
static void apply_handle_relation(StringInfo s)
```

## Detailed Description
This function handles RELATION messages that provide metadata about tables involved in logical replication. It reads the relation information from the message, updates the local relation mapping cache, and resets relevant partition map entries. The function employs a lazy validation strategy where schema validation against the local database is deferred until the first actual data change for the relation is processed, reducing locking overhead and improving performance.

The function first checks if this message is part of a streaming transaction and handles it appropriately through handle_streamed_transaction. For non-streaming contexts, it directly processes the relation metadata by updating the relation map and clearing partition map entries that reference the updated relation.

## Parameters / Member Variables
- `s`: StringInfo containing the RELATION message data with table metadata to be parsed and processed

## Dependencies
- Functions called/Symbols referenced:
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md) (streaming transaction management)
  - [logicalrep_read_rel](../l/logicalrep_read_rel.md) (parses relation data from message)
  - logicalrep_relmap_update (updates relation mapping cache)
  - [logicalrep_partmap_reset_relmap](../l/logicalrep_partmap_reset_relmap.md) (resets partition mapping entries)
  - LOGICAL_REP_MSG_RELATION (message type constant)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher)

## Notes and Other Information
- Static function used internally within the logical replication worker
- Implements deferred schema validation strategy to minimize locking
- Schema validation occurs only when first change for the relation is applied
- Updates both relation mapping and partition mapping data structures
- Handles both regular and streaming transaction contexts
- Part of PostgreSQL logical replication metadata management system
- Critical for maintaining consistent relation metadata between publisher and subscriber
- Partition map reset ensures consistency when relation definitions change