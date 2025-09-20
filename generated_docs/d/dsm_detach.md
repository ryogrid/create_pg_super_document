# dsm_detach

## Location
[src/backend/storage/ipc/dsm.c:803-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L803-L914)

## Overview
Detaches from a DSM segment, performing cleanup callbacks, unmapping memory, and destroying the segment if this was the last reference.

## Definition

```c
structures. */
	if (seg->resowner != NULL)
		ResourceOwnerForgetDSM(seg->resowner, seg);
```
## Detailed Description
The  function is the primary mechanism for cleanly disconnecting from a DSM segment. It performs a comprehensive cleanup process that ensures proper resource management and maintains system integrity. The function is designed to be robust and should never fail, as it's often called during error recovery scenarios.

The detachment process follows these steps:

1. **Callback execution**: Invokes any registered detach callbacks in a safe manner, protecting against infinite recursion and interrupts
2. **Memory unmapping**: Removes the segment mapping from the process's address space, handling both main region and system-level segments
3. **Reference counting**: Decrements the segment's reference count in the control structure
4. **Segment destruction**: If the reference count reaches 1 (indicating this was the last active reference), attempts to destroy the underlying segment
5. **Local cleanup**: Removes the segment from local data structures and frees associated memory

The function handles both main region segments (allocated from PostgreSQL's shared memory) and system-level segments (created via OS mechanisms) transparently. It includes extensive error handling and recovery mechanisms to ensure that detachment succeeds even in adverse conditions.

## Parameters / Member Variables
- : Pointer to the DSM segment descriptor to detach from

## Dependencies
- Functions called/Symbols referenced:
  - [slist_is_empty](../s/slist_is_empty.md)/slist_pop_head_node/slist_container (callback management)
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt control)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md) (segment type checking)
  - dsm_impl_op (platform-specific operations)
  - [FreePageManagerPut](../F/FreePageManagerPut.md) (main region deallocation)
  - ResourceOwnerForgetDSM (resource owner cleanup)
  - [dlist_delete](dlist_delete.md) (list management)
- Called from (representative examples):
  - [DetachSession](../D/DetachSession.md) (session management)
  - DestroyParallelContext (parallel query cleanup)
  - dsa_detach (dynamic shared arrays)
  - [dsm_backend_shutdown](dsm_backend_shutdown.md) (backend shutdown)

## Notes and Other Information
- Designed to never fail, even during error recovery scenarios
- Executes registered detach callbacks before performing detachment
- Uses WARNING level for platform operations to avoid cascading errors
- Handles segment destruction when reference count reaches 1
- Safe to call multiple times on the same segment
- Manages both memory unmapping and control structure cleanup
- Integrates with PostgreSQL's resource owner system for automatic cleanup
- Critical for preventing shared memory leaks in the DSM subsystem
- Reference count of 1 triggers destruction (0 means unused slot)