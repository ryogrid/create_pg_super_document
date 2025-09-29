# pgstat_get_backend_current_activity

## Location
[src/backend/utils/activity/backend_status.c:885-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L885-L962)

## Overview
Retrieves the current activity string of a specific backend process by PID, reading directly from shared memory to provide real-time information for diagnostic purposes.

## Definition
```c
const char *pgstat_get_backend_current_activity(int pid, bool checkUser)
```

## Detailed Description
This function provides real-time access to a backend's current activity string by searching the shared memory backend status array for a matching process ID. Unlike functions that use cached snapshots, this function reads directly from the live shared memory structure, making it suitable for scenarios where up-to-date information is critical, such as deadlock reporting.

The function implements the standard atomic read protocol to safely access the volatile shared memory while other processes may be updating it. It iterates through all backend slots until it finds the matching PID, then returns the current activity string with appropriate permission checks and formatting.

The function is specifically designed for use in diagnostic scenarios where the target backend is expected to be in a stable state (e.g., blocked on a lock). While there are race conditions possible if the target backend's state changes during access, the consequences are limited to potentially returning slightly stale information.

## Parameters / Member Variables
- `pid`: The process ID of the target backend to query
- `checkUser`: If true, enforces permission checks - non-superusers can only see their own activities

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md) (backend status structure type)
  - pgstat_begin_read_activity (atomic read protocol start)
  - pgstat_end_read_activity (atomic read protocol end)
  - pgstat_read_activity_complete (consistency verification)
  - [superuser](../s/superuser.md) (permission checking)
  - [pgstat_clip_activity](pgstat_clip_activity.md) (activity string formatting)
  - [GetUserId](../G/GetUserId.md) (current user identification)
- Called from:
  - [DeadLockReport](../D/DeadLockReport.md) (for deadlock diagnostic reporting)

## Notes and Other Information
- Returns special strings for various conditions:
  - "<insufficient privilege>" when user lacks permission to view the activity
  - "<command string not enabled>" when activity tracking is disabled
  - "<backend information not available>" when the PID is not found
- Designed for deadlock reporting where target backends are expected to be stable
- Uses direct shared memory access rather than cached snapshots for real-time information
- Implements memory leak tolerance ("this'll leak a bit of memory, but that seems acceptable")
- Return strings match those used by pg_stat_get_backend_activity for consistency
- Uses volatile pointer semantics to prevent compiler optimizations that could break atomic access

## Simplified Source

```c
const char *
pgstat_get_backend_current_activity(int pid, bool checkUser)
{
    PgBackendStatus *beentry;
    int i;

    // Search through all backend status entries
    beentry = BackendStatusArray;
    for (i = 1; i <= MaxBackends; i++) {
        volatile PgBackendStatus *vbeentry = beentry;
        bool found;

        // Use atomic read protocol to safely check PID
        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_begin_read_activity(vbeentry, before_changecount);
            found = (vbeentry->st_procpid == pid);
            pgstat_end_read_activity(vbeentry, after_changecount);

            if (pgstat_read_activity_complete(before_changecount, after_changecount))
                break;

            CHECK_FOR_INTERRUPTS();  // Allow breaking out if stuck
        }

        if (found) {
            // Check permissions
            if (checkUser && !superuser() && beentry->st_userid != GetUserId())
                return "<insufficient privilege>";
            else if (*(beentry->st_activity_raw) == '\0')
                return "<command string not enabled>";
            else
                return pgstat_clip_activity(beentry->st_activity_raw);
        }

        beentry++;
    }

    // PID not found
    return "<backend information not available>";
}
```