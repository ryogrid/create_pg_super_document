# pgstat_get_wait_event_type

## Location
[src/backend/utils/activity/wait_event.c:374-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L374-L431)

## Overview
Returns a string representing the current wait event type that a backend is waiting on, based on the wait event information.

## Definition
```c
const char *pgstat_get_wait_event_type(uint32 wait_event_info)
```

## Detailed Description
This function translates a numeric wait event information value into a human-readable string representing the wait event type. It extracts the class ID from the wait event information using a bitmask and then uses a switch statement to map the class ID to the corresponding wait event type name. The function handles all major wait event classes in PostgreSQL and returns "???" for unknown or invalid wait event types. If the wait event information is 0, it returns NULL indicating the process is not waiting.

## Parameters / Member Variables
- `wait_event_info`: A 32-bit unsigned integer containing wait event information from which to extract the type

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only constants and control structures)
- Constants used:
  - WAIT_EVENT_CLASS_MASK
  - PG_WAIT_LWLOCK
  - PG_WAIT_LOCK
  - PG_WAIT_BUFFERPIN
  - PG_WAIT_ACTIVITY
  - PG_WAIT_CLIENT
  - PG_WAIT_EXTENSION
  - PG_WAIT_IPC
  - PG_WAIT_TIMEOUT
  - PG_WAIT_IO
  - PG_WAIT_INJECTIONPOINT
- Called from (representative examples):
  - [pg_stat_get_backend_wait_event_type](pg_stat_get_backend_wait_event_type.md) (in pgstatfuncs.c:778)
  - [pg_isolation_test_session_is_blocked](pg_isolation_test_session_is_blocked.md) (in waitfuncs.c:59)
  - [WaitEventCustomNew](../W/WaitEventCustomNew.md) (in wait_event.c:209, 234)

## Notes and Other Information
- Returns NULL when wait_event_info is 0 (process not waiting)
- Returns "???" for unknown or invalid wait event class IDs
- Covers all major PostgreSQL wait event categories: LWLock, Lock, BufferPin, Activity, Client, Extension, IPC, Timeout, IO, and InjectionPoint
- Used extensively in PostgreSQL's monitoring and statistics reporting system
- The function performs bitwise masking to extract class information from the wait event info
- Located at src/backend/utils/activity/wait_event.c:374-431

## Simplified Source

```c
const char *
pgstat_get_wait_event_type(uint32 wait_event_info)
{
    uint32 classId;
    const char *event_type;

    // Return NULL if process is not waiting
    if (wait_event_info == 0)
        return NULL;

    // Extract wait event class from info
    classId = wait_event_info & WAIT_EVENT_CLASS_MASK;

    // Map class ID to event type name
    switch (classId) {
        case PG_WAIT_LWLOCK:
            event_type = "LWLock";
            break;
        case PG_WAIT_LOCK:
            event_type = "Lock";
            break;
        case PG_WAIT_BUFFERPIN:
            event_type = "BufferPin";
            break;
        case PG_WAIT_ACTIVITY:
            event_type = "Activity";
            break;
        case PG_WAIT_CLIENT:
            event_type = "Client";
            break;
        case PG_WAIT_EXTENSION:
            event_type = "Extension";
            break;
        case PG_WAIT_IPC:
            event_type = "IPC";
            break;
        case PG_WAIT_TIMEOUT:
            event_type = "Timeout";
            break;
        case PG_WAIT_IO:
            event_type = "IO";
            break;
        case PG_WAIT_INJECTIONPOINT:
            event_type = "InjectionPoint";
            break;
        default:
            event_type = "???";
            break;
    }

    return event_type;
}
```