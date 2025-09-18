# pgstat_beshutdown_hook

## Location
src/backend/utils/activity/backend_status.c: 440 - 466

## Overview
Process exit hook that clears the backend's entry in the shared PgBackendStatus array during process shutdown.

## Definition
```c
static void pgstat_beshutdown_hook(int code, Datum arg)
```

## Detailed Description
This static function serves as a cleanup hook that is automatically called when a backend process exits. It safely clears the process's entry in the shared backend status array by marking it as invalid (setting st_procpid to 0) and nullifying the local MyBEEntry pointer. The function follows PostgreSQL's standard protocol for modifying shared memory by using the PGSTAT write activity macros to ensure atomic updates.

## Parameters / Member Variables
- `code`: Exit code of the process (unused in this function)
- `arg`: Additional argument passed to the hook (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - PGSTAT_BEGIN_WRITE_ACTIVITY
  - PGSTAT_END_WRITE_ACTIVITY
  - MyBEEntry (global variable)
- Called from:
  - [pgstat_beinit](pgstat_beinit.md) (registered as exit hook)

## Notes and Other Information
This function is registered as a shared memory exit hook by pgstat_beinit and is automatically called during process shutdown. The function is declared static as it's only used internally within the backend_status.c module. Setting MyBEEntry to NULL at the end allows other functions to check if the backend status system is properly initialized by testing if MyBEEntry is non-NULL.