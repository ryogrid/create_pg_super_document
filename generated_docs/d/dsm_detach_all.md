# dsm_detach_all

## Location
[src/backend/storage/ipc/dsm.c:775-802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L775-L802)

## Overview
Detaches all DSM segments including the control segment, typically used in child processes that inherit mappings but should not maintain DSM connections.

## Definition


## Detailed Description
The  function provides a comprehensive cleanup mechanism that detaches all DSM segments from the current process, including the special control segment that manages the DSM subsystem itself. This function is primarily designed for use in child processes that may have inherited shared memory mappings from their parent but are not intended to participate in the dynamic shared memory system.

The function operates in two phases:
1. **Regular segment detachment**: Iterates through all attached DSM segments and detaches them using 
2. **Control segment detachment**: Explicitly detaches the DSM control segment using platform-specific operations

This complete detachment is essential for processes like utility programs or child processes that inherit the parent's memory mappings but should not maintain active connections to the DSM system. Unlike , this function also unmaps the control segment, making it suitable for scenarios where the process continues to run but should not access DSM.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](dlist_is_empty.md) (checks if segment list is empty)
  - dlist_head_element (gets first segment from list)
  - [dsm_detach](dsm_detach.md) (detaches individual segments)
  - dsm_impl_op (platform-specific detach operations for control segment)
- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (child process initialization)

## Notes and Other Information
- Intended for processes that inherit mappings but shouldn't use DSM
- More comprehensive than dsm_backend_shutdown() as it also detaches control segment
- Should be called alongside PGSharedMemoryDetach() for complete cleanup
- Safe to call in child processes that need to disconnect from DSM
- Used in postmaster child launch to prevent DSM inheritance
- Essential for proper process isolation in PostgreSQL's multi-process architecture
- Handles both regular segments and the special control segment
- Prevents child processes from accidentally accessing parent's DSM state