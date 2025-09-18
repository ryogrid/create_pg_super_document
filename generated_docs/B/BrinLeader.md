# BrinLeader

## Location
src/backend/access/brin/brin.c: 119 - 146

## Overview
BrinLeader is a structure that manages the leader process state during parallel BRIN index builds, providing convenience pointers to shared state and coordinating worker processes.

## Definition


## Detailed Description
BrinLeader serves as the control structure for the leader process in parallel BRIN index builds. It maintains references to the parallel execution context and provides convenient access to shared state that would otherwise require table of contents (TOC) lookups. The leader process uses this structure to coordinate with worker processes and manage the overall parallel build operation.

The structure includes pointers to shared memory segments for coordination, performance tracking (WAL and buffer usage), and snapshot management for MVCC consistency when required.

## Parameters / Member Variables
- : Pointer to the parallel execution context managing the parallel build
- : Total number of participant processes including successfully launched workers plus the leader if it participates as a worker
- : Convenience pointer to the shared state structure for the entire build process
- : Pointer to shared tuplesort-managed state passed to each process
- : MVCC snapshot used by the scan when MVCC snapshot is required
- : Pointer to WAL usage statistics for performance tracking
- : Pointer to buffer usage statistics for performance monitoring

## Dependencies
- Functions called/Symbols referenced:
  - ParallelContext
  - BrinShared
  - Sharedsort
  - WalUsage
  - BufferUsage
- Called from (representative examples):
  - BrinBuildState
  - _brin_begin_parallel
  - _brin_end_parallel
  - _brin_leader_participate_as_worker

## Notes and Other Information
The leader avoids TOC lookups by maintaining direct pointers to shared state. The nparticipanttuplesorts count excludes builds with DISABLE_LEADER_PARTICIPATION where the leader does not participate as a worker. The structure is essential for coordinating the parallel build process and collecting performance statistics.