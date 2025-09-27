# PGSharedMemoryCreate

## Location
[src/backend/port/sysv_shmem.c:700-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L700-L889)

## Overview
Creates a shared memory segment of specified size with proper initialization, dead segment recycling, and support for both System V and anonymous (mmap) shared memory types.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This is the main function for creating PostgreSQL's shared memory segment. It supports both System V shared memory and anonymous mmap-based memory, with the choice determined by the  configuration. The function uses the data directory's inode and device numbers to identify and potentially recycle dead segments from crashed processes.

For mmap mode, it creates an anonymous segment using CreateAnonymousSegment() and registers cleanup callbacks. For System V mode, it searches for an available IPC key, handling collisions with foreign segments and recycling dead PostgreSQL segments. The function validates huge page configurations and sets up appropriate memory allocation based on the selected memory type.

The function initializes a PGShmemHeader structure with metadata including creator PID, magic number, and data directory identification info. It handles various shared memory states including foreign segments, unattached segments from crashed processes, and segments still in use.

## Parameters / Member Variables
- : Requested size of the shared memory segment in bytes
- : Output parameter receiving pointer to the System V shared memory header (shim)

## Dependencies
- Functions called/Symbols referenced:
  - [CreateAnonymousSegment](../C/CreateAnonymousSegment.md)
  - [AnonymousShmemDetach](../A/AnonymousShmemDetach.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - [InternalIpcMemoryCreate](../I/InternalIpcMemoryCreate.md)
  - [PGSharedMemoryAttach](PGSharedMemoryAttach.md)
  - [dsm_cleanup_using_control_segment](../d/dsm_cleanup_using_control_segment.md)
  - [stat](../s/stat.md)
  - shmget
  - shmctl
  - shmdt
  - getpid
  - memcpy
- Constants referenced:
  - HUGE_PAGES_ON
  - SHMEM_TYPE_MMAP
  - Various SHMSTATE_* constants
  - PGC_INTERNAL
  - PGC_S_DYNAMIC_DEFAULT
  - IPC_RMID
- Called from (representative examples):
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)

## Notes and Other Information
- Supports both System V and anonymous mmap shared memory types
- Validates and enforces huge page configuration constraints (mmap-only)
- Recycles dead shared memory segments from crashed PostgreSQL processes
- Uses data directory inode/device as unique identifier and key seed
- Handles foreign shared memory segment collisions by trying next key
- Registers cleanup callback for anonymous segments via on_shmem_exit
- For mmap mode, creates minimal System V shim alongside anonymous segment
- Sets huge_pages_status configuration to reflect actual allocation method
- Returns pointer to actual memory (anonymous) or System V header depending on mode
- Initializes PGShmemHeader with creator PID, magic number, and data directory info
- Handles dynamic shared memory segment cleanup for recycled segments

## Simplified Source

```c
// Simplified version of PGSharedMemoryCreate
PGShmemHeader *
PGSharedMemoryCreate(Size size, PGShmemHeader **shim) {
    IpcMemoryKey NextShmemSegID;
    void *memAddress;
    PGShmemHeader *hdr;
    struct stat statbuf;
    Size sysvsize;

    // Get data directory stats for unique identification
    if (stat(DataDir, &statbuf) < 0)
        ereport(FATAL, "could not stat data directory");

    // Validate huge pages configuration
    if (huge_pages == HUGE_PAGES_ON && shared_memory_type != SHMEM_TYPE_MMAP)
        ereport(ERROR, "huge pages not supported with current shared_memory_type");

    // Determine memory allocation strategy
    if (shared_memory_type == SHMEM_TYPE_MMAP) {
        // Create anonymous mmap segment
        AnonymousShmem = CreateAnonymousSegment(&size);
        on_shmem_exit(AnonymousShmemDetach, 0);
        sysvsize = sizeof(PGShmemHeader);  // Just need small SysV shim
    } else {
        // Use full System V shared memory
        sysvsize = size;
        SetConfigOption("huge_pages_status", "off", PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
    }

    // Search for available IPC key, starting from data directory inode
    NextShmemSegID = statbuf.st_ino;

    for (;;) {
        IpcMemoryId shmid;
        PGShmemHeader *oldhdr;
        IpcMemoryState state;

        // Try to create new segment
        memAddress = InternalIpcMemoryCreate(NextShmemSegID, sysvsize);
        if (memAddress)
            break;  // Success - got our segment

        // Handle existing segment at this key
        shmid = shmget(NextShmemSegID, sizeof(PGShmemHeader), 0);
        if (shmid < 0) {
            state = SHMSTATE_FOREIGN;  // Foreign or non-existent segment
        } else {
            state = PGSharedMemoryAttach(shmid, NULL, &oldhdr);
        }

        switch (state) {
            case SHMSTATE_ATTACHED:
                // Still in use - fatal error
                ereport(FATAL, "shared memory block still in use");
                break;

            case SHMSTATE_FOREIGN:
                // Not ours - try next key
                NextShmemSegID++;
                break;

            case SHMSTATE_UNATTACHED:
                // Dead PostgreSQL segment - clean it up and retry
                if (oldhdr->dsm_control != 0)
                    dsm_cleanup_using_control_segment(oldhdr->dsm_control);
                if (shmctl(shmid, IPC_RMID, NULL) < 0)
                    NextShmemSegID++;  // Cleanup failed, try next key
                break;

            default:
                // Unexpected state - try next key
                NextShmemSegID++;
                break;
        }

        // Cleanup temporary attachment
        if (oldhdr && shmdt(oldhdr) < 0)
            elog(LOG, "shmdt failed");
    }

    // Initialize the new shared memory header
    hdr = (PGShmemHeader *) memAddress;
    hdr->creatorPID = getpid();
    hdr->magic = PGShmemMagic;
    hdr->dsm_control = 0;
    hdr->device = statbuf.st_dev;
    hdr->inode = statbuf.st_ino;
    hdr->totalsize = size;
    hdr->freeoffset = MAXALIGN(sizeof(PGShmemHeader));

    *shim = hdr;
    UsedShmemSegAddr = memAddress;
    UsedShmemSegID = NextShmemSegID;

    // Return appropriate pointer based on memory type
    if (AnonymousShmem == NULL) {
        return hdr;  // System V mode - return SysV header
    } else {
        // mmap mode - copy header to anonymous segment and return that
        memcpy(AnonymousShmem, hdr, sizeof(PGShmemHeader));
        return (PGShmemHeader *) AnonymousShmem;
    }
}
```

Key simplifications made:
- Removed detailed error handling and platform-specific checks for clarity
- Consolidated similar switch cases and error conditions
- Abstracted complex state checking logic into simpler flow
- Focused on the main execution paths for both mmap and SysV modes
- Simplified the segment recycling loop while preserving core logic
- Removed verbose error messages and detailed logging for readability