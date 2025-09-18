# ProcessClientReadInterrupt

## Location
src/backend/tcop/postgres.c: 513 - 558

## Overview
Processes interrupts specific to client reads, handling various interrupt conditions before and after low-level read operations.

## Definition
```c
void ProcessClientReadInterrupt(bool blocked)
```

## Detailed Description
ProcessClientReadInterrupt is a critical function in PostgreSQL's interrupt handling system that manages interrupts during client read operations. It is called just before and after low-level reads to ensure proper handling of various interrupt conditions while preserving the errno value. The function operates in two main modes depending on whether the backend is actively reading a command from the client (DoingCommandRead) or handling a process termination request (ProcDiePending).

When DoingCommandRead is true, the function performs comprehensive interrupt processing including general interrupts via CHECK_FOR_INTERRUPTS(), shared invalidation catchup interrupts, and notify interrupts. When the process is dying (ProcDiePending), it carefully manages the process latch to ensure safe termination while respecting the blocked state of the read operation.

## Parameters / Member Variables
- `blocked`: Boolean indicating whether no data was available to read and the operation will retry (true), or if the function is called before reading or after completing a read (false)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro)
  - [ProcessCatchupInterrupt](ProcessCatchupInterrupt.md)
  - [ProcessNotifyInterrupt](ProcessNotifyInterrupt.md)
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [secure_read](../s/secure_read.md) (in be-secure.c)
  - [interactive_getc](../i/interactive_getc.md) (in postgres.c)

## Notes and Other Information
- Must preserve errno value across the function call
- Critical for maintaining system responsiveness during client I/O operations
- Handles different interrupt types based on the current state of command processing
- Part of PostgreSQL's sophisticated interrupt handling mechanism that ensures safe and timely processing of system events during network operations