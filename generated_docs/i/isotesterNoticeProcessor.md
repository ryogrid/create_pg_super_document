# isotesterNoticeProcessor

## Location
src/test/isolation/isolationtester.c: 1126 - 1138

## Overview
A notice processor callback function that handles NOTICE messages from PostgreSQL database sessions during isolation testing.

## Definition


## Detailed Description
This function serves as a libpq notice processor callback that gets invoked whenever the PostgreSQL backend sends a NOTICE message to a client session during isolation testing. It performs three critical functions: formats the notice output by prefixing it with the session name for clarity, tracks the total count of notices received by incrementing the session's notice counter, and sets a global flag to indicate that new notices have arrived.

The notice tracking is particularly important for coordinating test step execution, as some steps may have PSB_NUM_NOTICES blocker conditions that wait for a specific number of notices before allowing the step to complete. The global  flag helps the step completion logic determine when to retry checking blocked steps.

## Parameters / Member Variables
- : Void pointer that should be cast to IsoConnInfo* representing the connection that received the notice
- : The notice message text received from the PostgreSQL backend

## Dependencies
- Functions called/Symbols referenced:
  - IsoConnInfo (structure type for connection information)
  - printf (standard library function)
  - any_new_notice (global flag variable)
- Called from (representative examples):
  - [main](../m/main.md) (registered as callback)
  - libpq notice processing system

## Notes and Other Information
- Registered as a notice processor callback using PQsetNoticeProcessor in main()
- Essential for step synchronization in tests that rely on NOTICE-based coordination
- The session name prefix helps identify which session generated each notice in test output
- Notice counting supports PSB_NUM_NOTICES blocker conditions for complex test scenarios
- Global  flag triggers retry logic in step completion functions
- Part of the broader isolation testing framework for ensuring deterministic test execution