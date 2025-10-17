# pg_isolation_test_session_is_blocked

## Location
[src/backend/utils/adt/waitfuncs.c:39-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/waitfuncs.c#L39-L113)

## Overview
A support function for the PostgreSQL isolation tester that determines if a specified process ID (PID) is blocked by any of the PIDs in a given list of interesting processes.

## Definition

```c
Datum
pg_isolation_test_session_is_blocked(PG_FUNCTION_ARGS)
```
## Detailed Description
This is an undocumented internal function specifically designed for use by PostgreSQL's isolation testing framework. The function checks whether a given session (identified by its PID) is blocked by any of the processes that are under the isolation tester's control.

The function performs three types of blocking checks:
1. **Injection Point Blocking**: Checks if the process is waiting at an injection point
2. **Heavyweight Lock Blocking**: Determines if the process is blocked waiting for heavyweight locks held by any of the interesting PIDs
3. **Safe Snapshot Blocking**: Checks if the process is waiting for a safe snapshot

The function is optimized to run efficiently with  by using naive search algorithms instead of more complex array operations that would trigger cache lookups.

## Parameters / Member Variables
-  (int32): The PID of the session to check for blocking
-  (ArrayType*): An array of int32 PIDs representing sessions under isolation tester control

## Dependencies
- Functions called/Symbols referenced:
  - : Get process structure for a given PID
  - : Get the wait event type for process statistics
  - : Get PIDs that are blocking heavyweight lock acquisition
  - : Get PIDs blocking safe snapshot acquisition
  - : Direct function call mechanism
  - : Convert Datum to ArrayType pointer
  - Array utility functions: , , , , 
  - : Check if array contains null values
  - : Atomic access macro
- Called from (representative examples):
  - No direct references found (used via SQL function calls from isolation tester)

## Notes and Other Information
- This function is intentionally undocumented and may change in future releases as required for testing purposes
- The function ignores blockage caused by PIDs not directly under the isolationtester's control (e.g., autovacuum processes)  
- Returns  if the blocked_pid session no longer exists (session gone means definitely unblocked)
- Uses a naive double-loop search algorithm for performance reasons when 
- For safe snapshot blocking, the function uses a simplified check that only determines if any blocking exists rather than checking specific PIDs
- Located in src/backend/utils/adt/waitfuncs.c:39-113

## Simplified Source

```c
Datum pg_isolation_test_session_is_blocked(PG_FUNCTION_ARGS) {
    int blocked_pid = PG_GETARG_INT32(0);
    ArrayType *interesting_pids_a = PG_GETARG_ARRAYTYPE_P(1);

    // Check if session exists and is waiting at injection point
    PGPROC *proc = BackendPidGetProc(blocked_pid);
    if (proc == NULL)
        PG_RETURN_BOOL(false);  // Session gone = unblocked

    const char *wait_event_type = pgstat_get_wait_event_type(UINT32_ACCESS_ONCE(proc->wait_event_info));
    if (wait_event_type && strcmp("InjectionPoint", wait_event_type) == 0)
        PG_RETURN_BOOL(true);

    // Extract interesting PIDs array
    if (array_contains_nulls(interesting_pids_a))
        elog(ERROR, "array must not contain nulls");

    int32 *interesting_pids = (int32 *) ARR_DATA_PTR(interesting_pids_a);
    int num_interesting_pids = ArrayGetNItems(ARR_NDIM(interesting_pids_a), ARR_DIMS(interesting_pids_a));

    // Get PIDs blocking heavyweight locks
    ArrayType *blocking_pids_a = DatumGetArrayTypeP(DirectFunctionCall1(pg_blocking_pids, blocked_pid));
    int32 *blocking_pids = (int32 *) ARR_DATA_PTR(blocking_pids_a);
    int num_blocking_pids = ArrayGetNItems(ARR_NDIM(blocking_pids_a), ARR_DIMS(blocking_pids_a));

    // Check if any blocking PIDs are in our interesting PIDs list
    // Use naive search for performance with debug_discard_caches
    for (int i = 0; i < num_blocking_pids; i++) {
        for (int j = 0; j < num_interesting_pids; j++) {
            if (blocking_pids[i] == interesting_pids[j])
                PG_RETURN_BOOL(true);
        }
    }

    // Check if blocked by safe snapshot acquisition
    int dummy;
    if (GetSafeSnapshotBlockingPids(blocked_pid, &dummy, 1) > 0)
        PG_RETURN_BOOL(true);

    PG_RETURN_BOOL(false);
}
```