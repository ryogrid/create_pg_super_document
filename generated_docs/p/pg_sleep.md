# pg_sleep

## Location
[src/backend/utils/adt/misc.c:370-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L370-L386)

## Overview
A PostgreSQL system function that pauses execution for a specified number of seconds, providing precise timing control with proper signal handling and interrupt support.

## Definition


## Detailed Description
The  function implements a delay mechanism that suspends execution for a specified floating-point number of seconds. It uses PostgreSQL's  mechanism to ensure responsive signal handling during the sleep period. The function is designed to handle interrupts gracefully and can wake up promptly when important signals like SIGALRM or SIGINT arrive.

The implementation uses a loop-based approach to handle sleep durations longer than the maximum WaitLatch delay (INT_MAX milliseconds). It sleeps in chunks of at most 10 minutes (600 seconds) and repeats until the total requested time has elapsed. This design prevents accumulation of timing errors across multiple sleep cycles.

## Parameters / Member Variables
-  (float8): The number of seconds to sleep, obtained via . Supports fractional seconds for precise timing.

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the floating-point argument from the function call
  - : Gets the current timestamp for time calculations
  - : Macro that converts current timestamp to floating-point seconds
  - : Checks for and handles pending interrupts
  - : Core PostgreSQL function for interruptible waiting
  - : Resets the latch after waiting
  - : Returns void result to the SQL engine

- Called from (representative examples):
  - SQL queries using  function
  - PostgreSQL built-in system functions catalog

## Notes and Other Information
- The function handles sleep durations longer than INT_MAX milliseconds by breaking them into 10-minute chunks
- Uses WaitLatch with flags  for proper signal handling
- The  event is used for monitoring and debugging purposes
- Timing accuracy is maintained by calculating the absolute end time initially rather than using relative delays
- The function can be interrupted by query cancellation or other PostgreSQL signals
- Located in  at lines 370-386