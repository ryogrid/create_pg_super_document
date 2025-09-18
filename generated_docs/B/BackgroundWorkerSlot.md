# BackgroundWorkerSlot

## Location
src/backend/postmaster/bgworker.c: 74 - 81

## Overview
BackgroundWorkerSlot is a shared memory structure that coordinates background worker processes between the postmaster and regular backend processes, implementing a lockless protocol for safe concurrent access.

## Definition


## Detailed Description
BackgroundWorkerSlot provides a thread-safe mechanism for managing background worker processes in PostgreSQL's shared memory architecture. The structure implements a sophisticated lockless protocol that allows the postmaster (which cannot take locks) to safely coordinate with regular backends that can take locks.

The key design principle is the handoff protocol controlled by the 'in_use' flag:
- When in_use=false: Regular backends own the slot and can modify it freely (postmaster ignores it)
- When in_use=true: Postmaster owns the slot and can examine it (backends cannot modify it)

This protocol ensures that the postmaster never crashes due to shared memory corruption, as it never takes locks that could become wedged. The structure supports dynamic background worker registration and termination while maintaining system stability.

## Parameters / Member Variables
- : Control flag for the handoff protocol; when false, backends control the slot; when true, postmaster controls it
- : Signal flag that backends can set even when slot is in use, telling postmaster not to restart the worker
- : Process ID of the background worker (InvalidPid if not started, 0 if dead)  
- : Counter incremented each time the slot is recycled, used to detect stale references
- : The actual BackgroundWorker configuration containing all worker-specific settings

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - BackgroundWorker
- Called from (representative examples):
  - BackgroundWorkerArray
  - BackgroundWorkerShmemSize
  - BackgroundWorkerShmemInit
  - RegisterDynamicBackgroundWorker
  - GetBackgroundWorkerPid
  - TerminateBackgroundWorker

## Notes and Other Information
- Requires careful memory barrier usage: backends must fully initialize slots and insert write barriers before setting in_use=true
- Backends accessing this structure must coordinate using BackgroundWorkerLock (exclusive for modification, shared for reading)
- The lockless protocol is critical for postmaster stability - postmaster corruption cannot wedge the system
- Generation counter prevents race conditions when slots are rapidly recycled
- Part of PostgreSQL's dynamic background worker infrastructure introduced for extensibility