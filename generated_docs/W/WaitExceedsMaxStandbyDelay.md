# WaitExceedsMaxStandbyDelay

## Location
[src/backend/storage/ipc/standby.c:233-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L233-L272)

## Overview
Implements a progressive backoff waiting mechanism for recovery conflicts, determining whether the maximum standby delay has been exceeded and providing controlled sleep intervals.

## Definition
```c
static bool WaitExceedsMaxStandbyDelay(uint32 wait_event_info)
```

## Detailed Description
This static function implements the standby wait logic used by ResolveRecoveryConflictWithVirtualXIDs. It checks if the current time has exceeded the maximum standby delay limit and performs progressive backoff sleeping to avoid busy-waiting. The function starts with a base sleep time (standbyWait_us) and doubles it on each call, capping at 1 second to ensure the process remains interruptible on all platforms.

## Parameters / Member Variables
- `wait_event_info`: uint32 - wait event identifier for progress reporting
- Returns: bool - true if wait time has been exceeded, false if waiting can continue

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (checks for pending interrupts)
  - [GetStandbyLimitTime](../G/GetStandbyLimitTime.md) (gets the cutoff time for conflicts)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (gets current system timestamp)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md) (reports start of wait to statistics system)
  - [pg_usleep](../p/pg_usleep.md) (sleeps for specified microseconds)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md) (reports end of wait to statistics system)
  - standbyWait_us (global variable tracking current sleep duration)
- Called from (representative examples):
  - [ResolveRecoveryConflictWithVirtualXIDs](../R/ResolveRecoveryConflictWithVirtualXIDs.md) (main conflict resolution function)

## Notes and Other Information
- Static function, only used within standby.c
- Implements exponential backoff: sleep time doubles each call, starting from standbyWait_us
- Maximum sleep time is capped at 1 second (1,000,000 microseconds) for platform compatibility
- Uses pg_usleep which may not be interruptible on some platforms, hence the 1s cap
- Integrates with PostgreSQL's wait event reporting system for monitoring
- Essential for avoiding busy-waiting during recovery conflict resolution
- Returns false to continue waiting, true when time limit exceeded