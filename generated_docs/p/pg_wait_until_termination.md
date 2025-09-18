# pg_wait_until_termination

## Location
[src/backend/storage/ipc/signalfuncs.c:148-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/signalfuncs.c#L148-L215)

## Overview
Static helper function that waits for a backend process to terminate within a specified timeout period, using efficient latch-based waiting with interrupt handling.

## Definition
```c
static bool pg_wait_until_termination(int pid, int64 timeout)
```

## Detailed Description
pg_wait_until_termination implements a sophisticated waiting mechanism for PostgreSQL backend process termination. It uses a polling approach combined with efficient latch-based waiting to avoid busy-looping while still being responsive to process termination and interrupts.

The function operates in 100-millisecond intervals, checking process existence using kill(pid, 0) and waiting via WaitLatch() between checks. This design provides:

1. **Efficient Waiting**: Uses PostgreSQL's latch mechanism instead of busy-waiting or sleep()
2. **Interrupt Responsiveness**: Processes pending interrupts (like query cancellation) during wait cycles
3. **Process Detection**: Uses kill(pid, 0) to detect when the target process no longer exists
4. **Timeout Handling**: Returns false with a warning message if the process doesn't terminate within the specified timeout
5. **Postmaster Death Detection**: Automatically exits if the postmaster dies during waiting

## Parameters / Member Variables
- `pid`: Process ID of the backend process to wait for termination
- `timeout`: Maximum time to wait in milliseconds before timing out

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call for process existence check)
  - CHECK_FOR_INTERRUPTS
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - MyLatch
  - ereport
  - [errmsg_plural](../e/errmsg_plural.md)
- Called from (representative examples):
  - [pg_terminate_backend](pg_terminate_backend.md)

## Notes and Other Information
- This is a static function, only accessible within signalfuncs.c
- Returns true if the process terminates within the timeout, false if it times out
- Uses a fixed 100-millisecond polling interval (waittime variable)
- Handles ESRCH errno to detect process termination
- Uses WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH flags for comprehensive wait handling
- Emits a pluralized warning message on timeout using errmsg_plural
- Designed to be interruptible - can be canceled by query cancellation or other interrupts
- Does not attempt to kill the process - only waits for its natural or externally-induced termination