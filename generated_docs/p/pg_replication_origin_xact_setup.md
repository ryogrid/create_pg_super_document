# pg_replication_origin_xact_setup

## Location
[src/backend/replication/logical/origin.c:1426-1443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1426-L1443)

## Overview
Sets up transaction-specific replication origin information by recording the remote LSN and timestamp for the current transaction being replicated.

## Definition
```c
Datum pg_replication_origin_xact_setup(PG_FUNCTION_ARGS)
```

## Detailed Description
This function configures transaction-specific replication origin state for the current transaction. It records both the remote LSN (Log Sequence Number) position and the timestamp from the original transaction on the remote server. This information is crucial for maintaining consistency and tracking the progress of logical replication.

The function stores the provided LSN and timestamp in session-level variables (replorigin_session_origin_lsn and replorigin_session_origin_timestamp) that will be used throughout the current transaction's processing. This allows PostgreSQL to properly track which remote transaction is being replicated and maintain accurate progress information.

The function ensures that a replication origin session is properly configured before allowing the setup to proceed, raising an error if no replication origin has been established.

## Parameters / Member Variables
- `location` (XLogRecPtr): The LSN position of the transaction on the remote/source server
- `timestamp` (TimestampTz): The commit timestamp of the original transaction on the remote server

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN
  - replorigin_check_prerequisites
  - PG_GETARG_TIMESTAMPTZ
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Must be called within an active replication origin session
- Sets global session variables replorigin_session_origin_lsn and replorigin_session_origin_timestamp
- Raises ERROR with code ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE if no replication origin is configured
- This function is typically called at the beginning of replaying a transaction from a remote source
- The stored LSN and timestamp information is used for progress tracking and conflict resolution
- Located in src/backend/replication/logical/origin.c:1426-1443