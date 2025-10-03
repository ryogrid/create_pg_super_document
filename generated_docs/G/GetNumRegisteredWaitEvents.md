# GetNumRegisteredWaitEvents

## Location
[src/backend/storage/ipc/latch.c:2269-2281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L2269-L2281)

## Overview
GetNumRegisteredWaitEvents is a simple accessor function that returns the number of events currently registered in a WaitEventSet.

## Definition

```c
int
GetNumRegisteredWaitEvents(WaitEventSet *set)
```
## Detailed Description
GetNumRegisteredWaitEvents provides a straightforward way to query the number of events that have been registered in a given WaitEventSet. The function simply returns the nevents field from the WaitEventSet structure, which tracks the current count of registered events.

This function is useful for code that needs to know how many events are currently being monitored in a wait set, perhaps for logging, debugging, or making decisions about whether to add more events or create additional wait sets.

## Parameters / Member Variables
- `set`: WaitEventSet to query for the number of registered events

## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventSet](../W/WaitEventSet.md) (struct type)
- Called from (representative examples):
  - [ExecAppendAsyncEventWait](../E/ExecAppendAsyncEventWait.md)

## Notes and Other Information
- Returns the current number of events registered in the WaitEventSet
- This is a simple getter function with no side effects
- The count includes all types of registered events (sockets, latches, postmaster death, etc.)
- Useful for monitoring and debugging wait event usage
- The returned count reflects the current state and may change if events are added or removed from the set

## Simplified Source

```c
int
GetNumRegisteredWaitEvents(WaitEventSet *set)
{
    return set->nevents;
}
```