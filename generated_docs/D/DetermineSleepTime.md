# DetermineSleepTime

## Location
src/backend/postmaster/postmaster.c: 1518 - 1602

## Overview
Calculates the optimal sleep duration in milliseconds for the PostgreSQL postmaster's ServerLoop to balance responsiveness with resource efficiency.

## Definition


## Detailed Description
DetermineSleepTime is a critical function in the postmaster's event loop that determines how long the server should wait before checking for new events. The function implements intelligent sleep duration calculation based on the current server state:

- **Normal operation**: Returns 60 seconds (60,000ms) to allow periodic maintenance tasks
- **Shutdown sequence**: Calculates remaining time before SIGKILL based on AbortStartTime
- **Background worker startup needed**: Returns 0 for immediate processing
- **Crashed background workers**: Calculates minimum time until next restart attempt

The function ensures that background workers are serviced promptly while maintaining efficient resource usage during normal operations. During shutdown, it provides a countdown mechanism for graceful termination before forceful process killing.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTzPlusMilliseconds
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)  
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md)
  - slist_foreach_modify
  - slist_container
- Constants used:
  - NoShutdown
  - SIGKILL_CHILDREN_AFTER_SECS
  - BGW_NEVER_RESTART
- Called from:
  - [ServerLoop](../S/ServerLoop.md)

## Notes and Other Information
- The function uses global variables like Shutdown, AbortStartTime, StartWorkerNeeded, and HaveCrashedWorker to determine server state
- Background worker restart timing is calculated using the worker's bgw_restart_time and crash timestamp
- The maximum sleep time is capped at 60 seconds to ensure periodic maintenance tasks execute
- During shutdown, the sleep time decreases as the SIGKILL deadline approaches
- Workers marked as BGW_NEVER_RESTART or flagged for termination are removed from the background worker list