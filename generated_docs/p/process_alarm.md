# process_alarm

## Location
src/bin/pg_test_fsync/pg_test_fsync.c: 642 - 648

## Overview
A signal handler function in pg_test_fsync that sets a global flag when an alarm signal is received, enabling timed testing operations.

## Definition


## Detailed Description
The process_alarm function serves as a signal handler specifically for SIGALRM signals in the pg_test_fsync utility. When the alarm timer expires, this handler is invoked and sets the global alarm_triggered flag to true. This mechanism allows pg_test_fsync to implement time-bounded testing where operations continue until a specified duration elapses. The function provides a clean way to interrupt ongoing benchmark loops when the allocated testing time has expired, ensuring consistent test durations across different performance measurements.

## Parameters / Member Variables
- Uses  macro which provides standard signal handler parameters (signal number, signal info, context)

## Dependencies
- Functions called/Symbols referenced:
  - alarm_triggered (global boolean flag set when alarm occurs)
  - SIGNAL_ARGS (macro providing signal handler parameters)
- Called from (representative examples):
  - STOP_TIMER macro (alarm signal handler registration)
  - main function (signal handler setup)

## Notes and Other Information
- Extremely simple handler that only sets a global flag
- Works in conjunction with alarm() system call for timed testing
- Global alarm_triggered flag is checked by benchmark loops to determine when to stop
- Essential for implementing consistent test durations in pg_test_fsync
- Must be signal-safe as it runs in signal context
- Part of the timing infrastructure that enables reproducible benchmark results