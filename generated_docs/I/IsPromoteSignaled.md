# IsPromoteSignaled

## Location
[src/backend/postmaster/startup.c:288-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L288-L293)

## Overview
IsPromoteSignaled is a simple accessor function that returns the current state of the promote signal flag, indicating whether a promotion from standby to primary server has been requested.

## Definition
```c
bool IsPromoteSignaled(void)
```

## Detailed Description
IsPromoteSignaled provides a clean interface to check whether a promotion signal has been received by the startup process. In PostgreSQL's streaming replication architecture, a standby server can be promoted to become the primary server. This promotion is typically triggered by external tools or administrators sending specific signals to the startup process.

The function simply returns the value of the global promote_signaled flag, which is set by signal handlers when promotion-related signals are received. This allows other parts of the recovery system to check promotion status without directly accessing global variables, maintaining better code organization and encapsulation.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value.

## Dependencies
- Functions called/Symbols referenced:
  - None (only accesses global variable)
- Global variables used:
  - promote_signaled (returned as the result)
- Called from (representative examples):
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md) (in xlogrecovery.c)

## Notes and Other Information
- This function provides read-only access to the promotion signal state
- The promote_signaled flag is typically set by signal handlers in response to SIGUSR2 or similar promotion triggers
- Essential for standby server promotion logic in PostgreSQL's replication system
- The function follows PostgreSQL's pattern of providing accessor functions for global state variables
- Used during recovery to determine when to transition from standby to primary mode
- Part of the broader standby server promotion mechanism that enables failover scenarios

## Simplified Source

```c
bool
IsPromoteSignaled(void)
{
    // Return the current state of the promotion signal flag
    return promote_signaled;
}
```