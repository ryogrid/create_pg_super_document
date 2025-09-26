# WaitForParallelWorkersToExit

## Location
[src/backend/access/transam/parallel.c:906-945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L906-L945)

## Overview
Waits for all parallel workers to completely shut down and handles cleanup of background worker resources.

## Definition
```c
static void WaitForParallelWorkersToExit(ParallelContext *pcxt)
```

## Detailed Description
This static function ensures complete shutdown of all parallel workers by waiting for their actual termination, not just the completion of their work. It provides the final cleanup phase of parallel operation lifecycle:

1. **Complete Shutdown**: Unlike WaitForParallelWorkersToFinish which only ensures message completion, this function waits for actual process termination
2. **Resource Cleanup**: Releases background worker handles and associated memory
3. **Postmaster Failure Handling**: Detects if the postmaster has died during worker shutdown and issues a FATAL error
4. **Memory Management**: Properly frees allocated background worker handle memory

The function is called internally by DestroyParallelContext to ensure all workers have completely exited before final cleanup.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext containing worker information and handles to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - WaitForBackgroundWorkerShutdown
  - ereport (with FATAL level)
  - pfree
- Called from (representative examples):
  - DestroyParallelContext
  - ReinitializeParallelDSM

## Notes and Other Information
- Static function, not part of the public API
- Critical for preventing resource leaks and ensuring clean worker shutdown
- Issues FATAL error if postmaster dies during shutdown, as safe cleanup becomes impossible
- Runs with HOLD_INTERRUPTS/RESUME_INTERRUPTS in calling context for transaction safety
- Handles edge case where workers may still be running when parallel context is being destroyed