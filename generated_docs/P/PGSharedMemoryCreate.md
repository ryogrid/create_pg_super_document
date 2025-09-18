# PGSharedMemoryCreate

## Location
src/backend/port/sysv_shmem.c: 700 - 889

## Overview
Creates a shared memory segment of specified size with proper initialization, dead segment recycling, and support for both System V and anonymous (mmap) shared memory types.

## Definition


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
  - dsm_cleanup_using_control_segment
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