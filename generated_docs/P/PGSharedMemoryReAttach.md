# PGSharedMemoryReAttach

## Location
[src/backend/port/sysv_shmem.c:890-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L890-L938)

## Overview
Re-attaches a postmaster child process to an existing shared memory segment in the EXEC_BACKEND configuration, where child processes don't inherit shared memory through fork().

## Definition

```c
void
PGSharedMemoryReAttach(void)
```
## Detailed Description
This function is specifically designed for the EXEC_BACKEND case where postmaster child processes need to explicitly re-attach to the shared memory segment that was created by the postmaster. In normal configurations, child processes inherit the shared memory attachment through fork(), making this function unnecessary.

The function performs the following key operations:
1. Uses the global variables UsedShmemSegID and UsedShmemSegAddr (restored by the caller) to identify the target shared memory segment
2. On Cygwin systems, explicitly detaches from any existing attachment before re-attaching
3. Attempts to get the shared memory segment using shmget() with the stored segment ID
4. Calls PGSharedMemoryAttach() to perform the actual attachment
5. Validates that the attachment succeeded and returned the expected memory address
6. Sets up the dynamic shared memory control handle for the attached segment

The function includes critical error checking to ensure the re-attachment process succeeds and that the returned memory address matches the expected location.

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- : The shared memory segment identifier (implicit parameter)
- : The expected memory address for attachment (implicit parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [PGSharedMemoryDetach](PGSharedMemoryDetach.md) (for Cygwin cleanup)
  - shmget (system call to get shared memory segment)
  - [PGSharedMemoryAttach](PGSharedMemoryAttach.md) (performs actual attachment)
  - [dsm_set_control_handle](../d/dsm_set_control_handle.md) (sets up dynamic shared memory control)
  - elog (error logging)
- Called from:
  - [SubPostmasterMain](../S/SubPostmasterMain.md)

## Notes and Other Information
- Only used in EXEC_BACKEND configurations where fork() inheritance is not available
- Requires Assert(UsedShmemSegAddr != NULL) and Assert(IsUnderPostmaster) to ensure proper calling context
- Special handling for Cygwin systems due to cygipc behavior with exec()
- Fatal errors are raised if re-attachment fails or returns unexpected memory addresses
- The function is critical for maintaining shared memory consistency across process boundaries in EXEC_BACKEND environments

## Simplified Source

```c
void PGSharedMemoryReAttach(void) {
    IpcMemoryId shmid;
    PGShmemHeader *hdr;
    IpcMemoryState state;
    void *origUsedShmemSegAddr = UsedShmemSegAddr;

    Assert(UsedShmemSegAddr != NULL);
    Assert(IsUnderPostmaster);

#ifdef __CYGWIN__
    // Cygwin-specific: explicit detach before reattach
    PGSharedMemoryDetach();
    UsedShmemSegAddr = origUsedShmemSegAddr;
#endif

    // Get shared memory segment
    elog(DEBUG3, "attaching to %p", UsedShmemSegAddr);
    shmid = shmget(UsedShmemSegID, sizeof(PGShmemHeader), 0);

    if (shmid < 0)
        state = SHMSTATE_FOREIGN;
    else
        state = PGSharedMemoryAttach(shmid, UsedShmemSegAddr, &hdr);

    // Verify successful attachment
    if (state != SHMSTATE_ATTACHED)
        elog(FATAL, "could not reattach to shared memory (key=%d, addr=%p): %m",
             (int) UsedShmemSegID, UsedShmemSegAddr);

    if (hdr != origUsedShmemSegAddr)
        elog(FATAL, "reattaching to shared memory returned unexpected address (got %p, expected %p)",
             hdr, origUsedShmemSegAddr);

    // Setup dynamic shared memory control
    dsm_set_control_handle(hdr->dsm_control);
    UsedShmemSegAddr = hdr;
}
```