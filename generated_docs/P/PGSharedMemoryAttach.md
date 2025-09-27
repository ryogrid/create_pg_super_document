# PGSharedMemoryAttach

## Location
[src/backend/port/sysv_shmem.c:347-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L347-L478)

## Overview
PGSharedMemoryAttach attempts to attach to an existing System V shared memory segment and analyzes its state to determine if it belongs to the current PostgreSQL data directory.

## Definition
```c
static IpcMemoryState PGSharedMemoryAttach(IpcMemoryId shmId, void *attachAt, PGShmemHeader **addr)
```

## Detailed Description
This static function provides comprehensive analysis of shared memory segments to determine their state and ownership. It performs the following operations:

1. **Existence Check**: Uses shmctl() with IPC_STAT to verify the segment exists
2. **Data Directory Validation**: Stats the current DataDir to get device and inode information  
3. **Attachment Attempt**: Uses shmat() to attach to the segment at the specified or system-chosen address
4. **Ownership Verification**: Compares the segment's magic number, device, and inode against the current data directory
5. **Usage Analysis**: Checks if other processes are currently attached to the segment

The function handles various error conditions gracefully, including platform-specific quirks (like the Linux EIDRM bug), and provides detailed state information through the IpcMemoryState return value.

## Parameters / Member Variables
- `shmId`: The System V shared memory identifier to attach to
- `attachAt`: Preferred attachment address (NULL for system choice, recommended for probing)
- `addr`: Output parameter - set to the attached segment address or NULL

## Dependencies
- Functions called/Symbols referenced:
  - shmctl (System V IPC function)
  - shmat (System V IPC function)  
  - [stat](../s/stat.md) (file system function)
  - PGShmemMagic (PostgreSQL shared memory magic number)
  - DataDir (global variable)
  - Various IpcMemoryState enum values
- Called from (representative examples):
  - [PGSharedMemoryIsInUse](PGSharedMemoryIsInUse.md)
  - [PGSharedMemoryCreate](PGSharedMemoryCreate.md)
  - [PGSharedMemoryReAttach](PGSharedMemoryReAttach.md)

## Notes and Other Information
- This is a static function, only accessible within the sysv_shmem.c file
- Implements robust error handling for various System V IPC error conditions
- Includes workarounds for Linux kernel bugs (HAVE_LINUX_EIDRM_BUG)
- Uses device and inode comparison to avoid false matches with segments from other data directories
- The magic number, device, and inode verification ensures the segment belongs to the current PostgreSQL instance
- Returns different IpcMemoryState values: SHMSTATE_ENOENT (doesn't exist), SHMSTATE_FOREIGN (belongs to different instance), SHMSTATE_UNATTACHED (exists but no processes attached), SHMSTATE_ATTACHED (active), SHMSTATE_ANALYSIS_FAILURE (couldn't determine state)
- Critical for preventing conflicts between multiple PostgreSQL instances on the same system

## Simplified Source

```c
// Simplified version of PGSharedMemoryAttach
static IpcMemoryState
PGSharedMemoryAttach(IpcMemoryId shmId, void *attachAt, PGShmemHeader **addr)
{
    struct shmid_ds shmStat;
    struct stat statbuf;
    PGShmemHeader *hdr;

    *addr = NULL;

    // Step 1: Check if shared memory segment exists
    if (shmctl(shmId, IPC_STAT, &shmStat) < 0) {
        if (errno == EINVAL || errno == EIDRM) // Segment doesn't exist
            return SHMSTATE_ENOENT;
        if (errno == EACCES) // No permissions, not our segment
            return SHMSTATE_FOREIGN;
        return SHMSTATE_ANALYSIS_FAILURE; // Other errors
    }

    // Step 2: Get data directory info for ownership verification
    if (stat(DataDir, &statbuf) < 0)
        return SHMSTATE_ANALYSIS_FAILURE;

    // Step 3: Attempt to attach to the shared memory segment
    hdr = (PGShmemHeader *) shmat(shmId, attachAt, PG_SHMAT_FLAGS);
    if (hdr == (PGShmemHeader *) -1) {
        // Attachment failed - similar error handling as above
        if (errno == EINVAL || errno == EIDRM)
            return SHMSTATE_ENOENT;
        if (errno == EACCES)
            return SHMSTATE_FOREIGN;
        return SHMSTATE_ANALYSIS_FAILURE;
    }
    *addr = hdr;

    // Step 4: Verify this segment belongs to our PostgreSQL instance
    if (hdr->magic != PGShmemMagic ||
        hdr->device != statbuf.st_dev ||
        hdr->inode != statbuf.st_ino) {
        return SHMSTATE_FOREIGN; // Not our segment
    }

    // Step 5: Check if other processes are attached
    return shmStat.shm_nattch == 0 ? SHMSTATE_UNATTACHED : SHMSTATE_ATTACHED;
}
```

Key simplifications made:
- Removed detailed platform-specific error handling comments
- Consolidated error conditions for readability
- Simplified Linux EIDRM bug handling into main error check
- Added step-by-step comments explaining the main logic flow
- Focused on the core algorithm: check existence → verify ownership → determine usage state
- Maintained all essential functionality and return values