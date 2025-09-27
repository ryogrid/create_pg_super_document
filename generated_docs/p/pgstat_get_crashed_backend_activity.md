# pgstat_get_crashed_backend_activity

## Location
[src/backend/utils/activity/backend_status.c:963-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L963-L1026)

## Overview
Safely retrieves the activity string of a crashed backend process for postmaster logging, with special handling for potentially corrupted shared memory.

## Definition
```c
const char *pgstat_get_crashed_backend_activity(int pid, char *buffer, int buflen)
```

## Detailed Description
This specialized function is designed exclusively for use by the postmaster process to extract activity information from backends that have crashed. Unlike normal backend status reading functions, this function operates under the assumption that shared memory may be corrupted due to the crash, requiring extra safety measures.

The function intentionally bypasses the normal concurrency protocols used for accessing the BackendStatusArray, since the target backend is no longer running and cannot be updating its status. However, this means the function must be extremely careful about memory access validation to avoid crashes in the postmaster itself.

Key safety features include bounds checking to ensure the activity pointer falls within the valid BackendActivityBuffer range, ASCII-only character copying to avoid encoding issues in crash reports, and avoiding any operations that might trigger ereport(ERROR) since error handling may be compromised during crash recovery.

## Parameters / Member Variables
- `pid`: Process ID of the crashed backend to query
- `buffer`: Caller-provided buffer to copy the activity string into
- `buflen`: Size of the provided buffer

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md) (backend status structure type)
  - [ascii_safe_strlcpy](../a/ascii_safe_strlcpy.md) (safe ASCII-only string copying)
  - BackendActivityBuffer (global shared memory activity buffer)
  - BackendActivityBufferSize (size of activity buffer)
  - pgstat_track_activity_query_size (configuration for query string size)
- Called from:
  - [LogChildExit](../L/LogChildExit.md) (postmaster crash logging)

## Notes and Other Information
- **Postmaster-only function**: Specifically designed for postmaster use during crash reporting
- **No concurrency protocol**: Deliberately skips normal atomic read protocols since target backend is crashed
- **Corruption-tolerant**: Designed to handle potentially corrupted shared memory safely
- **ASCII-safe output**: Filters output to ASCII characters only to prevent encoding issues in crash logs
- **Bounds validation**: Carefully validates that activity pointers fall within expected memory ranges
- **Error-avoidance**: Avoids operations that might trigger ereport(ERROR) during crash handling
- Returns NULL on any safety concern rather than risking postmaster stability
- Buffer size is limited to the smaller of caller's buffer or configured activity size
- Does not attempt multibyte-aware character handling due to crash context constraints

## Simplified Source

```c
// Simplified version of pgstat_get_crashed_backend_activity
const char *pgstat_get_crashed_backend_activity(int pid, char *buffer, int buflen) {
    volatile PgBackendStatus *beentry;
    int i;

    beentry = BackendStatusArray;

    // Safety check: ensure shared memory is initialized
    if (beentry == NULL || BackendActivityBuffer == NULL) {
        return NULL;
    }

    // Search for the backend with matching PID
    for (i = 1; i <= MaxBackends; i++) {
        if (beentry->st_procpid == pid) {
            // Read activity pointer once to prevent TOCTOU issues
            const char *activity = beentry->st_activity_raw;
            const char *activity_last;

            // Calculate safe bounds for activity string access
            activity_last = BackendActivityBuffer + BackendActivityBufferSize
                - pgstat_track_activity_query_size;

            // Validate that activity pointer is within valid buffer range
            if (activity < BackendActivityBuffer || activity > activity_last) {
                return NULL;
            }

            // Skip empty activity strings
            if (activity[0] == '\0') {
                return NULL;
            }

            // Copy activity string with ASCII-only safety filtering
            ascii_safe_strlcpy(buffer, activity,
                              Min(buflen, pgstat_track_activity_query_size));

            return buffer;
        }

        beentry++;
    }

    // Backend PID not found
    return NULL;
}
```

Key simplifications made:
- Added descriptive comments explaining each safety check
- Clarified the purpose of bounds validation
- Explained the TOCTOU (Time-of-Check-Time-of-Use) protection
- Simplified the memory safety logic explanation
- Maintained all critical safety features for crash handling
- Preserved ASCII-only filtering for encoding safety