# asyncQueueFillWarning

## Location
[src/backend/commands/async.c:1527-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1527-L1580)

## Overview
Internal function that monitors notification queue utilization and emits warnings when the queue becomes at least 50% full, with rate limiting to prevent excessive warning messages.

## Definition


## Detailed Description
This function serves as a monitoring and alerting mechanism for the asynchronous notification queue. It checks if the queue utilization has reached or exceeded 50% capacity and, if so, emits a warning message to help administrators identify potential performance issues. The function includes rate limiting to ensure warnings are shown at most once every QUEUE_FULL_WARN_INTERVAL. When issuing a warning, it identifies the backend process with the oldest transaction that is preventing queue cleanup and provides specific guidance about the situation.

## Parameters / Member Variables
- No parameters

## Dependencies
- Functions called/Symbols referenced:
  - [asyncQueueUsage](asyncQueueUsage.md) (gets current queue utilization)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (gets current time for rate limiting)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md) (checks if enough time has passed since last warning)
  - QUEUE_FULL_WARN_INTERVAL (constant defining minimum time between warnings)
  - [QueuePosition](../Q/QueuePosition.md), QUEUE_HEAD (queue position tracking)
  - QUEUE_FIRST_LISTENER, QUEUE_NEXT_LISTENER (iterate through listening backends)
  - QUEUE_BACKEND_PID, QUEUE_BACKEND_POS (access backend information)
  - QUEUE_POS_MIN, QUEUE_POS_EQUAL (queue position comparison macros)
  - ereport (PostgreSQL error/warning reporting system)
- Called from:
  - [PreCommit_Notify](../P/PreCommit_Notify.md) (checks for queue fill warnings before committing notifications)
  - NotificationHash (context reference in async.c)

## Notes and Other Information
- Caller must hold exclusive NotifyQueueLock before calling
- Warning threshold is set at 50% queue capacity
- Rate limiting prevents warning spam using QUEUE_FULL_WARN_INTERVAL
- Identifies the specific backend process (PID) with the oldest transaction blocking queue cleanup
- Provides detailed error messages with hints for resolution
- Updates lastQueueFillWarn timestamp in asyncQueueControl after issuing warning
- Internal static function, not exposed outside async.c module
- Part of PostgreSQL's proactive monitoring system to prevent notification queue overflow