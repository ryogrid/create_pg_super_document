# ReqCheckpointHandler

## Location
src/backend/postmaster/checkpointer.c: 862 - 881

## Overview
A signal handler function that responds to SIGINT signals by triggering the checkpointer process to wake up and perform a normal checkpoint.

## Definition
```c
static void ReqCheckpointHandler(SIGNAL_ARGS)
```

## Detailed Description
This is a minimal signal handler that serves as part of PostgreSQL's checkpointer process communication mechanism. When a SIGINT signal is received, it simply sets the process latch to wake up the main checkpointer loop. The actual checkpoint flags and parameters are expected to be set by the signaling process before sending the signal.

The handler is designed to be signal-safe and performs only the minimal necessary action - waking up the main loop by setting the latch. All the actual checkpoint logic and flag checking happens in the main checkpointer loop after it wakes up.

## Parameters / Member Variables
- Uses `SIGNAL_ARGS` macro which typically expands to signal number parameter

## Dependencies
- Functions called/Symbols referenced:
  - SetLatch
  - MyLatch (global variable)
- Called from (representative examples):
  - Registered as signal handler in CheckpointerMain

## Notes and Other Information
- This is a signal handler function, so it must be async-signal-safe
- The function assumes that `ckpt_flags` has been set by the signaling process
- Does not perform any actual checkpoint work - only wakes up the main loop
- Part of the inter-process communication mechanism for checkpoint coordination
- Registered to handle SIGINT signals in the checkpointer process