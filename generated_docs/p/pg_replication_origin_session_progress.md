# pg_replication_origin_session_progress

## Location
[src/backend/replication/logical/origin.c:1405-1425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1405-L1425)

## Overview
Returns the replication progress for the origin setup in the current session, representing the remote LSN (Log Sequence Number) position of the last replicated transaction.

## Definition


## Detailed Description
This PostgreSQL function retrieves the current replication progress for a replication origin that has been configured in the current session. The function returns the remote LSN (Log Sequence Number) that represents the position of the last transaction that was successfully replicated from the remote source. 

The function accepts a flush parameter that determines whether the returned LSN corresponds to a transaction that has been flushed to disk. When flush is true, it ensures that the returned value represents a durable state, which is particularly important when asynchronous commits are used during replication.

The function performs prerequisite checks to ensure that replication origins are properly configured and that a replication origin session is active. If no replication origin is configured in the current session, it raises an error.

## Parameters / Member Variables
-  (bool): When true, ensures the returned LSN corresponds to a transaction that has been flushed to disk, providing durability guarantees

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - replorigin_check_prerequisites  
  - [replorigin_session_get_progress](../r/replorigin_session_get_progress.md)
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL if no remote LSN is available (InvalidXLogRecPtr)
- Raises ERROR with code ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE if no replication origin is configured
- The flush parameter is particularly useful when using asynchronous commits during logical replication
- This function is typically exposed as a SQL function for monitoring replication progress
- Located in src/backend/replication/logical/origin.c:1405-1425