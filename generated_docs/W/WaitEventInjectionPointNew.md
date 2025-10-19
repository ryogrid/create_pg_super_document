# WaitEventInjectionPointNew

## Location
[src/backend/utils/activity/wait_event.c:170-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L170-L175)

## Overview
Creates a new wait event for injection points with the specified name, or returns existing event information if the name is already defined.

## Definition

```c
uint32
WaitEventInjectionPointNew(const char *wait_event_name)
```
## Detailed Description
This function provides a specialized interface for PostgreSQL's injection point testing framework to create custom wait events. It serves as a wrapper around WaitEventCustomNew(), automatically categorizing the wait event under the PG_WAIT_INJECTIONPOINT class.

Injection points are a testing and debugging mechanism that allows developers to insert controllable wait points or other behaviors into PostgreSQL's code execution flow. This function creates wait events specifically for these injection points, enabling monitoring and analysis of injection point behavior during testing or debugging sessions.

Like other wait event creation functions, this ensures name uniqueness - if a wait event with the given name already exists within the injection point namespace, it returns the existing event's information rather than creating a duplicate entry.

## Parameters / Member Variables
- `*wait_event_name`: A null-terminated string containing the name of the injection point wait event to create or retrieve. The name should be descriptive and correspond to the specific injection point being monitored.
## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventCustomNew](WaitEventCustomNew.md) (underlying custom wait event creation function)
  - PG_WAIT_INJECTIONPOINT (wait event class constant for injection points)

- Called from (representative examples):
  - [injection_wait](../i/injection_wait.md) (in src/test/modules/injection_points/injection_points.c:219)
  - PG_WAIT_INJECTIONPOINT (in src/include/utils/wait_event.h:59)

## Notes and Other Information
- This function is specifically designed for PostgreSQL's injection point testing framework
- Injection points are primarily used for testing, debugging, and controlled execution flow modification
- The returned uint32 value is a wait event ID that can be used with pgstat_report_wait_start() and pgstat_report_wait_end()
- Wait events created by this function will appear in system monitoring views with the 'InjectionPoint' wait event type
- This function is typically used in testing modules and debugging scenarios rather than in production code
- The function is thread-safe and can be called from multiple processes concurrently
- Injection point wait events help developers understand timing and synchronization behavior during testing

## Simplified Source

```c
uint32 WaitEventInjectionPointNew(const char *wait_event_name) {
    // Create a new wait event specifically for injection points
    return WaitEventCustomNew(PG_WAIT_INJECTIONPOINT, wait_event_name);
}
```