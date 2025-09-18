# DropAllPreparedStatements

## Location
src/backend/commands/prepare.c: 537 - 567

## Overview
Drops all cached prepared statements by iterating through the prepared statement hash table and releasing their associated plan cache entries.

## Definition
```c
void DropAllPreparedStatements(void)
```

## Detailed Description
This function provides a mechanism to clear all prepared statements from the session's cache. It walks through the entire prepared_queries hash table, releasing each cached plan via DropCachedPlan() and removing the corresponding hash table entry. The function is designed to handle cases where a complete cleanup of prepared statements is required, such as during session termination or when explicitly discarding all prepared statements.

The function safely handles the case where no prepared statements exist by checking if the prepared_queries hash table is initialized before proceeding with the cleanup operation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search  
  - DropCachedPlan
  - hash_search
  - HASH_REMOVE
- Data structures used:
  - HASH_SEQ_STATUS
  - PreparedStatement
  - prepared_queries (global hash table)
- Called from (representative examples):
  - DiscardAll
  - DeallocateQuery

## Notes and Other Information
- The function uses PostgreSQL's hash table sequential scan mechanism to safely iterate through all entries
- Each prepared statement's plan cache is properly released before removing the hash table entry to prevent memory leaks
- The function is safe to call even when no prepared statements exist
- This operation cannot be rolled back as it involves releasing cached plans
- Typically used in scenarios requiring complete session cleanup or explicit discard operations