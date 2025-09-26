# BarrierInit

## Location
src/backend/storage/ipc/barrier.c: 100 - 124

## Overview
Initializes a barrier synchronization structure to coordinate multiple backend processes, supporting both static and dynamic participant counts.

## Definition


## Detailed Description
BarrierInit sets up a barrier synchronization primitive that allows multiple PostgreSQL backend processes to synchronize at specific points in their execution. The barrier can operate in two modes:

1. **Static party mode**: When , the barrier is configured for a fixed number of participants that are implicitly attached and expected to arrive at each synchronization phase.

2. **Dynamic party mode**: When , the barrier starts with no attached participants, and backends must explicitly attach and detach using  and /.

The function initializes all barrier state variables to their starting values, sets up the spinlock for thread-safe access, and initializes the condition variable used for process synchronization.

## Parameters / Member Variables
- : Pointer to the Barrier structure to initialize
- : Number of participants for static mode (> 0), or 0 for dynamic mode

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockInit
  - ConditionVariableInit
  - Barrier (struct type)
- Called from (representative examples):
  - ExecParallelHashJoinSetUpBatches
  - ExecHashJoinInitializeDSM
  - ExecHashJoinReInitializeDSM

## Notes and Other Information
- The barrier structure includes a  field used primarily for assertions to ensure proper usage patterns
- All barrier state counters (phase, arrived, elected) are initialized to zero
- The mutex provides thread-safe access to barrier state during synchronization operations
- This is the mandatory first step before any barrier can be used for process synchronization