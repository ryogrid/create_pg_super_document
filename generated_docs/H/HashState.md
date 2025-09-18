# HashState

## Location
src/include/nodes/execnodes.h: 2744 - 2767

## Overview
HashState is the execution state structure for Hash nodes in PostgreSQL's executor, maintaining the hash table and related information for hash joins and other hash-based operations.

## Definition


## Detailed Description
HashState represents the runtime state for Hash executor nodes in PostgreSQL. It extends PlanState to provide hash-specific functionality and maintains the hash table used during hash join operations. The structure supports both single-process and parallel hash joins, with special handling for shared memory coordination in parallel scenarios. It tracks hash statistics for performance monitoring and optimization purposes.

## Parameters / Member Variables
-   PID TTY          TIME CMD
12693 ?        00:00:00 bash
12720 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node information
- : The actual hash table data structure used for hash join operations
- : List of ExprState nodes representing the hash key expressions
- : Pointer to shared memory statistics area (leader process only in parallel joins)
- : Hash instrumentation data for collecting performance statistics
- : State information for coordinating parallel hash join execution

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinTable](HashJoinTable.md)
  - [SharedHashInfo](../S/SharedHashInfo.md)
  - [HashInstrumentation](HashInstrumentation.md)
  - ParallelHashJoinState
- Called from (representative examples):
  - [MultiExecHash](../M/MultiExecHash.md)
  - [ExecInitHash](../E/ExecInitHash.md)
  - ExecEndHash
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md)

## Notes and Other Information
- The structure is specifically designed to handle both sequential and parallel hash operations
- In parallel hash joins, only the leader process maintains shared_info, while workers have this field as NULL
- Hash instrumentation can use either local or shared memory depending on the execution context
- The structure is central to PostgreSQL's hash join implementation and performance monitoring