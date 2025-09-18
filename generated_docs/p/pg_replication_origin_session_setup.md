# pg_replication_origin_session_setup

## Location
src/backend/replication/logical/origin.c: 1350 - 1371

## Overview
SQL-callable function that sets up a replication origin for the current session, enabling the session to track and manage replication progress for a specific origin.

## Definition
```c
Datum pg_replication_origin_session_setup(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function wrapper for establishing a replication origin session. It takes a text parameter containing the origin name, looks up the corresponding origin ID, and sets up the session state to track replication progress for that specific origin. The function initializes shared memory structures and caches access to the origin's replication slot for efficient subsequent operations. Once set up, the session can track LSN progress and coordinate with other replication processes.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: `text` - Name of the replication origin to set up for this session

## Dependencies
- Functions called/Symbols referenced:
  - `RepOriginId` - Type definition for replication origin identifiers
  - `replorigin_check_prerequisites` - Validates prerequisites with `check_slots=true` and `recoveryOK=false`
  - `text_to_cstring` - Converts PostgreSQL text datum to null-terminated C string
  - `replorigin_by_name` - Looks up origin ID by name with `missing_ok=false` (throws error if not found)
  - [replorigin_session_setup](../r/replorigin_session_setup.md) - Performs the actual session setup with `acquired_by=0`
  - `replorigin_session_origin` - Global variable set to track the current session's origin
  - [pfree](pfree.md) - Frees allocated memory for the converted string
  - `PG_RETURN_VOID` - Returns void result to PostgreSQL function call framework
- Called from (representative examples):
  - SQL interface (no direct C callers found)

## Notes and Other Information
- Requires `max_replication_slots > 0` (checked by prerequisites function)
- Cannot be called during recovery (enforced by prerequisites check)
- Only one origin can be set up per session - attempting to set up another origin without first calling reset will result in an error
- Uses `acquired_by=0` meaning the slot cannot be already acquired by another process
- Sets up shared memory structures for tracking replication progress
- Registers cleanup handlers to ensure proper resource cleanup on session exit
- Part of PostgreSQL's logical replication origin session management system
- The session setup enables subsequent calls to origin tracking functions
- Located in `src/backend/replication/logical/origin.c:1350-1371`