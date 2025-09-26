# DropAllPreparedStatements

## Location
[src/backend/commands/prepare.c:537-567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L537-L567)

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
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)  
  - [DropCachedPlan](DropCachedPlan.md)
  - [hash_search](../h/hash_search.md)
  - HASH_REMOVE
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [PreparedStatement](../P/PreparedStatement.md)
  - prepared_queries (global hash table)
- Called from (representative examples):
  - [DiscardAll](DiscardAll.md)
  - [DeallocateQuery](DeallocateQuery.md)

## Notes and Other Information
- The function uses PostgreSQL's hash table sequential scan mechanism to safely iterate through all entries
- Each prepared statement's plan cache is properly released before removing the hash table entry to prevent memory leaks
- The function is safe to call even when no prepared statements exist
- This operation cannot be rolled back as it involves releasing cached plans
- Typically used in scenarios requiring complete session cleanup or explicit discard operations