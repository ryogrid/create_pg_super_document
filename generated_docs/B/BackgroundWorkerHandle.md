# BackgroundWorkerHandle

## Location
[src/backend/postmaster/bgworker.c:102-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L102-L145)

## Overview
BackgroundWorkerHandle is a lightweight reference structure that provides safe access to background worker slots using a slot index and generation number to prevent stale references.

## Definition

```c
struct BackgroundWorkerHandle
{
	int			slot;
	uint64		generation;
};
```
## Detailed Description
BackgroundWorkerHandle serves as an opaque handle that allows client code to safely reference dynamically registered background workers. The structure implements a generation-based approach to detect stale references when worker slots are recycled.

The handle contains two key pieces of information:
- A slot index into the BackgroundWorkerArray to locate the specific worker slot
- A generation counter that must match the slot's generation to ensure the reference is still valid

This design prevents race conditions where a slot might be recycled and reused for a different background worker between the time a handle is created and when it's later used. The generation counter is incremented each time a slot is recycled, making stale handles detectable.

## Parameters / Member Variables
- `slot`: Index into the BackgroundWorkerArray slot array to identify which worker slot this handle references
- `generation`: Generation counter that must match the corresponding BackgroundWorkerSlot's generation to ensure validity

## Dependencies
- Functions called/Symbols referenced:
  - [BackgroundWorkerArray](BackgroundWorkerArray.md) (implicitly through slot indexing)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md)
  - [ApplyWorkerMain](../A/ApplyWorkerMain.md)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)
  - [TablesyncWorkerMain](../T/TablesyncWorkerMain.md)
  - [LookupBackgroundWorkerFunction](../L/LookupBackgroundWorkerFunction.md)
- Called from (representative examples):
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md)
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - [WaitForBackgroundWorkerStartup](../W/WaitForBackgroundWorkerStartup.md)
  - [WaitForBackgroundWorkerShutdown](../W/WaitForBackgroundWorkerShutdown.md)
  - [TerminateBackgroundWorker](../T/TerminateBackgroundWorker.md)
  - logicalrep_worker_launch

## Notes and Other Information
- Provides safe abstraction for background worker references without exposing internal slot management details
- Generation-based validity checking prevents use-after-free scenarios in dynamic worker management
- Widely used throughout PostgreSQL's parallel execution and logical replication systems
- Essential component for shared memory queue (shm_mq) communication with background workers
- Enables reliable communication patterns between main backend processes and their spawned background workers
- Part of the public API for extensions that need to manage background workers dynamically