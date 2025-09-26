# pg_replication_origin_progress

## Location
[src/backend/replication/logical/origin.c:1491-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1491-L1515)

## Overview
Returns the replication progress for an individual replication origin identified by name, providing the remote LSN position of the last successfully replicated transaction.

## Definition
```c
Datum pg_replication_origin_progress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the current replication progress for a specific replication origin identified by its name. It returns the remote LSN (Log Sequence Number) that represents the position of the last transaction that was successfully replicated from the remote source for this particular origin.

The function accepts a flush parameter that determines whether the returned LSN corresponds to a transaction that has been flushed to disk. When flush is true, it ensures that the returned value represents a durable state, which is particularly important when asynchronous commits are used during replication.

The function performs prerequisite checks to ensure replication origins are properly configured, looks up the origin by name to get its internal ID, and then retrieves the progress information using the internal replorigin_get_progress() function. If no progress has been recorded (InvalidXLogRecPtr), it returns NULL.

## Parameters / Member Variables
- `name` (text): The name of the replication origin to query for progress
- `flush` (bool): When true, ensures the returned LSN corresponds to a transaction that has been flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - [replorigin_check_prerequisites](../r/replorigin_check_prerequisites.md)
  - [text_to_cstring](../t/text_to_cstring.md)
  - PG_GETARG_BOOL
  - [replorigin_by_name](../r/replorigin_by_name.md)
  - [replorigin_get_progress](../r/replorigin_get_progress.md)
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL if no remote LSN is available (InvalidXLogRecPtr)
- Uses Assert to verify that the origin ID is valid after lookup
- The flush parameter is particularly useful when using asynchronous commits during logical replication
- This function provides progress monitoring for any named replication origin, not just session-specific origins
- Can be used to monitor replication lag and progress across multiple origins
- This is the general-purpose counterpart to pg_replication_origin_session_progress which only works for session-configured origins
- Located in src/backend/replication/logical/origin.c:1491-1515