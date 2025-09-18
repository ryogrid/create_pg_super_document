# pg_usleep

## Location
src/port/pgsleep.c: 41 - 57

## Overview
A Windows-specific sleep function that delays execution for a specified number of microseconds while remaining responsive to PostgreSQL signal events.

## Definition


## Detailed Description
 is a Windows-specific implementation of a microsecond sleep function that provides signal-aware sleeping behavior. This function replaces the non-signal-aware version provided by  on Windows platforms.

The function implements two key behaviors:
1. **Early startup handling**: If called before the signal event system is initialized ( is NULL), it falls back to a regular non-interruptible  call.
2. **Signal-aware sleeping**: When the signal system is active, it uses  to wait on the signal event, allowing the sleep to be interrupted by PostgreSQL signals.

The microsecond values are converted to milliseconds using the formula , which provides proper rounding and ensures a minimum sleep time of 1 millisecond.

When a signal is received during sleep, the function:
- Calls  to process pending signals
- Sets  to indicate the sleep was interrupted
- Returns immediately

## Parameters / Member Variables
- : The number of microseconds to sleep. Values less than 500 microseconds are rounded up to 1 millisecond, while larger values are converted to milliseconds with proper rounding.

## Dependencies
- Functions called/Symbols referenced:
  -  - Processes queued PostgreSQL signals when sleep is interrupted
  -  - Standard errno value set when sleep is interrupted by a signal
  -  - Windows API function used for fallback non-interruptible sleep
  -  - Windows API function used for signal-aware waiting
  -  - Global event object used for Windows signal handling

- Called from (representative examples):
  -  - Transaction log flushing operations
  -  - Checkpoint creation process
  -  - Vacuum delay mechanism
  -  - Background writer process
  -  - WAL writer process
  -  - Spinlock delay mechanism
  - Various other PostgreSQL processes and utilities

## Notes and Other Information
- This is a Windows-specific implementation located in 
- The function is designed to integrate with PostgreSQL's Windows signal handling system
- Early in startup, before signal handling is initialized, it provides basic sleep functionality
- The conversion from microseconds to milliseconds ensures compatibility with Windows API timing requirements
- This function is critical for PostgreSQL's ability to respond to signals on Windows while maintaining timing-sensitive operations
- Used extensively throughout PostgreSQL for implementing delays that need to be interruptible by signals