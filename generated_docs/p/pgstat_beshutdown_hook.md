# pgstat_beshutdown_hook

## Location
[src/backend/utils/activity/backend_status.c:440-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L440-L466)

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

## Simplified Source

```c
// Simplified version of pgstat_beshutdown_hook
static void pgstat_beshutdown_hook(int exit_code, Datum unused_arg) {
    volatile PgBackendStatus *backend_entry = MyBEEntry;

    // Step 1: Begin atomic write operation to shared backend status
    PGSTAT_BEGIN_WRITE_ACTIVITY(backend_entry);

    // Step 2: Mark this backend entry as invalid
    backend_entry->st_procpid = 0;

    // Step 3: Complete atomic write operation
    PGSTAT_END_WRITE_ACTIVITY(backend_entry);

    // Step 4: Clear local reference to indicate shutdown complete
    MyBEEntry = NULL;
}
```

Key simplifications made:
- Renamed parameters for clarity (code -> exit_code, arg -> unused_arg)
- Renamed variable for clarity (beentry -> backend_entry)
- Added step-by-step comments explaining the atomic write protocol
- Simplified the comments while preserving the essential volatile pointer usage
- Maintained the critical atomic update protocol for shared memory safety
- Preserved the final MyBEEntry nullification for system state checking