# WaitEventExtensionNew

## Location
[src/backend/utils/activity/wait_event.c:164-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L164-L169)

## Overview
Creates a new wait event for extensions with the specified name, or returns existing event information if the name is already defined.

## Definition

```c
uint32
WaitEventExtensionNew(const char *wait_event_name)
```
## Detailed Description
This function provides a convenient interface for PostgreSQL extensions to create custom wait events. It serves as a wrapper around WaitEventCustomNew(), automatically categorizing the wait event under the PG_WAIT_EXTENSION class.

The function ensures that wait event names are unique within the extension namespace - if a wait event with the given name already exists, it returns the existing event's information rather than creating a duplicate. This behavior allows extensions to safely call this function multiple times with the same name without causing conflicts.

Extensions can use the returned wait event ID to monitor specific waiting conditions in their code, which will then appear in PostgreSQL's wait event monitoring views like pg_stat_activity.

## Parameters / Member Variables
- `*wait_event_name`: A null-terminated string containing the name of the wait event to create or retrieve. The name should be descriptive and unique within the extension's namespace.
## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventCustomNew](WaitEventCustomNew.md) (underlying custom wait event creation function)
  - PG_WAIT_EXTENSION (wait event class constant for extensions)

- Called from (representative examples):
  - [wait_for_workers_to_become_ready](../w/wait_for_workers_to_become_ready.md) (in src/test/modules/test_shm_mq/setup.c:286)
  - [test_shm_mq_pipelined](../t/test_shm_mq_pipelined.md) (in src/test/modules/test_shm_mq/test.c:233)
  - [worker_spi_main](../w/worker_spi_main.md) (in src/test/modules/worker_spi/worker_spi.c:216)
  - PG_WAIT_INJECTIONPOINT (in src/include/utils/wait_event.h:58)

## Notes and Other Information
- This function is specifically designed for use by PostgreSQL extensions
- The returned uint32 value is a wait event ID that can be used with pgstat_report_wait_start() and pgstat_report_wait_end()
- Wait event names should follow PostgreSQL naming conventions and be descriptive enough to help with debugging and monitoring
- The function is thread-safe and can be called from multiple processes concurrently
- Extensions should typically call this function during initialization to register their wait events
- The wait event will appear in system monitoring views with the 'Extension' wait event type