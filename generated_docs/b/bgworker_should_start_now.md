# bgworker_should_start_now

## Location
[src/backend/postmaster/postmaster.c:4305-4346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4305-L4346)

## Overview
Determines whether a background worker should be started based on the current postmaster state and the worker's configured start time.

## Definition
```c
static bool bgworker_should_start_now(BgWorkerStartTime start_time)
```

## Detailed Description
This function implements the logic for determining when background workers should be allowed to start based on PostgreSQL's postmaster state machine. It maps background worker start time requirements to specific postmaster states:

- **BgWorkerStart_PostmasterStart**: Workers that start immediately when postmaster starts
- **BgWorkerStart_ConsistentState**: Workers that start when database reaches consistent state (hot standby)  
- **BgWorkerStart_RecoveryFinished**: Workers that start only after recovery is complete

The function uses a cascading switch statement where later states inherit the ability to start workers from earlier states, reflecting PostgreSQL's state progression during startup and recovery.

## Parameters / Member Variables
- `start_time`: BgWorkerStartTime enum value specifying when the worker should be allowed to start

## Dependencies
- Functions called/Symbols referenced:
  - [BgWorkerStartTime](../B/BgWorkerStartTime.md) (enum type)
  - BgWorkerStart_PostmasterStart
  - BgWorkerStart_ConsistentState  
  - BgWorkerStart_RecoveryFinished
  - PM_* state constants (postmaster states)
- Called from (representative examples):
  - MAX_BGWORKERS_TO_LAUNCH (referenced in worker startup logic)

## Notes and Other Information
- Returns true if worker should start now, false otherwise
- Uses fall-through switch logic to implement state hierarchy
- Workers with later start times can start in earlier postmaster states
- Shutdown and termination states (PM_SHUTDOWN, PM_STOP_BACKENDS, etc.) never allow new worker starts
- Critical for ensuring background workers start at appropriate times during database lifecycle

## Simplified Source

```c
static bool
bgworker_should_start_now(BgWorkerStartTime start_time)
{
    switch (pmState)
    {
        // Shutdown states - no workers should start
        case PM_NO_CHILDREN:
        case PM_WAIT_DEAD_END:
        case PM_SHUTDOWN_2:
        case PM_SHUTDOWN:
        case PM_WAIT_BACKENDS:
        case PM_STOP_BACKENDS:
            break;

        // Normal running state - allow recovery-finished workers
        case PM_RUN:
            if (start_time == BgWorkerStart_RecoveryFinished)
                return true;
            /* fall through */

        // Hot standby state - allow consistent-state workers
        case PM_HOT_STANDBY:
            if (start_time == BgWorkerStart_ConsistentState)
                return true;
            /* fall through */

        // Early states - allow postmaster-start workers
        case PM_RECOVERY:
        case PM_STARTUP:
        case PM_INIT:
            if (start_time == BgWorkerStart_PostmasterStart)
                return true;
            /* fall through */
    }

    return false;
}
```