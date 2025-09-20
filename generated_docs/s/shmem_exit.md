# shmem_exit

## Location
[src/backend/storage/ipc/ipc.c:228-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L228-L293)

## Overview
The shmem_exit function orchestrates shared memory cleanup during PostgreSQL process termination, executing callbacks in a carefully ordered sequence to ensure proper resource deallocation.

## Definition

```c
void
shmem_exit(int code)
```
## Detailed Description
shmem_exit implements a three-phase shared memory cleanup strategy during process termination. It first executes before_shmem_exit callbacks for high-level cleanup operations that require system functionality (like catalog access for temporary relation cleanup), then calls dsm_backend_shutdown() for dynamic shared memory cleanup, and finally runs on_shmem_exit callbacks for low-level shared memory resource release. The function sets and clears the shmem_exit_inprogress flag to track cleanup state and prevent reentrancy issues. Unlike proc_exit(), this function does not actually terminate the process but only performs shared memory cleanup, making it suitable for use by the postmaster during backend recovery operations.

## Parameters / Member Variables
- : Exit status code passed to all cleanup callbacks

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_backend_shutdown](../d/dsm_backend_shutdown.md) (dynamic shared memory cleanup)
  - elog (for debugging output)
- Called from (representative examples):
  - [proc_exit_prepare](../p/proc_exit_prepare.md) (normal process termination)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) (postmaster recovery operations)
  - PG_END_ENSURE_ERROR_CLEANUP (error cleanup macro)

## Notes and Other Information
- Does not actually exit the process, only performs shared memory cleanup
- Used by postmaster to clean up after backend crashes for shared memory reinitialization
- Three-phase cleanup: before_shmem_exit → dynamic shared memory → on_shmem_exit
- [before_shmem_exit](../b/before_shmem_exit.md) callbacks run first and need most system functionality available
- Dynamic shared memory cleanup is explicitly called rather than registered as callback
- [on_shmem_exit](../o/on_shmem_exit.md) callbacks handle low-level resource cleanup and serve as backstop
- Callbacks are removed from lists before execution to prevent infinite loops on errors
- Sets shmem_exit_inprogress flag during execution to track cleanup state
- All callback arrays are reset to index 0 after execution