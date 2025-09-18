# CheckConnection

## Location
src/bin/psql/common.c: 342 - 402

## Overview
CheckConnection verifies the database connection status and attempts automatic reconnection if the connection has been lost, handling both interactive and non-interactive scenarios.

## Definition


## Detailed Description
CheckConnection is a comprehensive connection management function that ensures psql maintains a valid database connection. It first calls ConnectionUp() to check the current connection status. If the connection is lost, the function's behavior depends on whether psql is running in interactive mode. In non-interactive mode, it logs an error and exits with EXIT_BADCONN. In interactive mode, it attempts to restore the connection using PQreset(). If the reset succeeds, it re-synchronizes variables and shows connection warnings. If the reset fails, it transitions to a disconnected state by storing the failed connection in pset.dead_conn for potential future reference, cleaning up the current connection state, and resetting related components. This function is critical for maintaining robust database connectivity throughout psql sessions.

## Parameters / Member Variables
- No parameters (void function)
- Returns: bool indicating whether a valid connection exists after the check/recovery attempt

## Dependencies
- Functions called/Symbols referenced:
  - [ConnectionUp](ConnectionUp.md) (checks if connection is active)
  - [PQreset](../P/PQreset.md) (libpq function to reset/reconnect)
  - [PQfinish](../P/PQfinish.md) (libpq function to close connections)
  - [ResetCancelConn](../R/ResetCancelConn.md) (resets cancellation connection state)
  - [UnsyncVariables](../U/UnsyncVariables.md) (cleans up variable synchronization)
  - [SyncVariables](../S/SyncVariables.md) (re-synchronizes variables after reconnection)
  - [connection_warnings](../c/connection_warnings.md) (displays connection-related warnings)
  - pg_log_error (logs error messages)
  - EXIT_BADCONN (exit code for bad connections)
- Called from (representative examples):
  - [AcceptResult](../A/AcceptResult.md) (before processing query results)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (before executing queries)

## Notes and Other Information
- This is a static function, only accessible within the common.c compilation unit
- Handles both interactive and non-interactive psql sessions differently
- Maintains state consistency by keeping failed connections in pset.dead_conn for later reference
- Automatically attempts reconnection in interactive mode to improve user experience
- Critical for ensuring reliable database operations throughout psql sessions
- Works closely with connection state management functions to maintain consistency
- Part of psql's robust error handling and recovery infrastructure