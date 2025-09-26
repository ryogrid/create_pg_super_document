# DetachSession

## Location
[src/backend/access/common/session.c:201-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/session.c#L201-L208)

## Overview
Detaches the current backend from the session DSM segment, cleaning up shared memory resources and running detach hooks.

## Definition
void DetachSession(void)

## Detailed Description
DetachSession explicitly detaches the current backend from the session DSM segment and DSA area, clearing the CurrentSession references. While not strictly necessary since backends automatically detach at exit, this function is important for scenarios where parallel workers might be reused across different sessions.

The function performs the following operations:
1. Calls dsm_detach() on the current session's DSM segment, which runs any registered detach hooks
2. Sets CurrentSession->segment to NULL to clear the reference
3. Calls dsa_detach() on the current session's DSA area
4. Sets CurrentSession->area to NULL to clear the reference

The detach hooks are crucial as they allow subsystems to perform cleanup when the session is being torn down, ensuring proper resource management and state consistency.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_detach](../d/dsm_detach.md)
  - [dsa_detach](../d/dsa_detach.md)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (in src/backend/access/transam/parallel.c:1562)

## Notes and Other Information
- Explicitly runs detach hooks through dsm_detach(), allowing subsystems to clean up session-specific state
- Not strictly required as backends automatically detach at exit, but important for worker reuse scenarios
- Nullifies CurrentSession references to prevent access to detached resources
- Should be called by worker processes when they finish working on a particular session
- Does not deallocate the CurrentSession object itself, only detaches from shared resources
- Future worker reuse scenarios will require proper detachment before attaching to new sessions