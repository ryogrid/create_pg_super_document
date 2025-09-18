# HotStandbyActiveInReplay

## Location
[src/backend/access/transam/xlogrecovery.c:4528-4539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4528-L4539)

## Overview
A specialized version of HotStandbyActive() designed specifically for use within WAL replay code, providing direct access to Hot Standby status without inter-process communication.

## Definition
```c
static bool HotStandbyActiveInReplay(void)
```

## Detailed Description
This function provides a streamlined way to check Hot Standby status specifically within the WAL replay context. Unlike the general-purpose HotStandbyActive() function, this version doesn't need to acquire spinlocks or check shared memory because it's designed to be called only from the startup process or in non-postmaster environments where the local state is authoritative.

The function includes an assertion to ensure it's only called from appropriate contexts (startup process or non-postmaster environments). This makes it more efficient than the general HotStandbyActive() function when used in the correct context, as it avoids the overhead of spinlock acquisition and shared memory access.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating Hot Standby status.

## Dependencies
- Functions called/Symbols referenced:
  - AmStartupProcess (assertion check)
  - IsPostmasterEnvironment (assertion check)
  - LocalHotStandbyActive (local static variable)
- Called from (representative examples):
  - [RecoveryRequiresIntParameter](../R/RecoveryRequiresIntParameter.md)

## Notes and Other Information
- Static function - only accessible within the same source file (xlogrecovery.c)
- Optimized for WAL replay code where inter-process communication is unnecessary
- Must only be called from startup process or non-postmaster environments
- More efficient than HotStandbyActive() in appropriate contexts due to lack of spinlock overhead
- Part of PostgreSQL's Hot Standby infrastructure for enabling read-only queries during recovery