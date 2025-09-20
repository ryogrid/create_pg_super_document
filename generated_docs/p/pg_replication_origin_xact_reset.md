# pg_replication_origin_xact_reset

## Location
[src/backend/replication/logical/origin.c:1444-1455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1444-L1455)

## Overview
Resets transaction-specific replication origin information by clearing the stored remote LSN and timestamp values.

## Definition
```c
Datum pg_replication_origin_xact_reset(PG_FUNCTION_ARGS)
```

## Detailed Description
This function clears transaction-specific replication origin state that was previously set up by pg_replication_origin_xact_setup. It resets both the remote LSN (Log Sequence Number) and timestamp to their invalid/null states, effectively clearing any transaction-specific replication tracking information.

The function sets replorigin_session_origin_lsn to InvalidXLogRecPtr and replorigin_session_origin_timestamp to 0, which indicates that no remote transaction information is currently being tracked. This is typically called after a replicated transaction has been completed or when preparing to start a new transaction replay.

Unlike pg_replication_origin_xact_setup, this function does not require a replication origin session to be active, making it safe to call during cleanup operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - replorigin_check_prerequisites
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Resets global session variables replorigin_session_origin_lsn to InvalidXLogRecPtr and replorigin_session_origin_timestamp to 0
- Does not require an active replication origin session (unlike pg_replication_origin_xact_setup)
- Typically called after completing replication of a transaction or during cleanup
- This function is the counterpart to pg_replication_origin_xact_setup
- Safe to call multiple times or when no transaction setup is active
- Located in src/backend/replication/logical/origin.c:1444-1455