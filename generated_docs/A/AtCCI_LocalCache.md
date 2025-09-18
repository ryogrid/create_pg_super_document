# AtCCI_LocalCache

## Location
src/backend/access/transam/xact.c: 1558 - 1576

## Overview
AtCCI_LocalCache handles local cache invalidation during command counter increment operations, ensuring that catalog changes become visible within the current transaction for subsequent commands.

## Definition
```c
static void AtCCI_LocalCache(void)
```

## Detailed Description
This static function is called as part of the command counter increment (CCI) process to make catalog changes visible locally within the current transaction. It performs a two-step process: first making any pending relation map changes visible through AtCCI_RelationMap(), and then processing local invalidation messages to update the local catalog caches via CommandEndInvalidationMessages(). The ordering is critical - relation map changes must be processed before invalidation messages so that when relcache invalidations are processed, they can properly reflect the updated relation mappings.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [AtCCI_RelationMap](AtCCI_RelationMap.md)
  - [CommandEndInvalidationMessages](../C/CommandEndInvalidationMessages.md)
- Called from (representative examples):
  - CommandCounterIncrement

## Notes and Other Information
- This is a static function within xact.c, part of the command counter increment mechanism
- Critical for maintaining transaction isolation and ensuring catalog changes are visible within the same transaction
- The function operates on local caches and invalidation messages, not global state
- Essential for PostgreSQL's multi-version concurrency control (MVCC) implementation
- The two-step process (relation map then invalidation messages) ensures proper ordering of cache updates
- Part of PostgreSQL's mechanism to ensure that DDL changes become visible to subsequent commands within the same transaction