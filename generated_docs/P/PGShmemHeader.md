# PGShmemHeader

## Location
src/include/storage/pg_shmem.h: 29 - 42

## Overview
PGShmemHeader is a standard header structure that defines the metadata for all PostgreSQL shared memory segments, providing essential information for shared memory management and identification.

## Definition


## Detailed Description
PGShmemHeader serves as a standardized header structure placed at the beginning of every PostgreSQL shared memory segment. This structure provides crucial metadata that enables PostgreSQL processes to identify, validate, and manage shared memory segments. The header includes a magic number for validation, process identification, size information, and platform-specific filesystem metadata for additional validation on Unix-like systems.

The structure is designed to be portable across different platforms while providing essential shared memory management capabilities. On non-Windows platforms, it includes device and inode information to ensure that the shared memory segment corresponds to the correct PostgreSQL data directory.

## Parameters / Member Variables
- : A magic number (679834894) used to identify valid PostgreSQL shared memory segments and distinguish them from other shared memory segments
- : Process ID of the process that created the shared memory segment (stored for informational purposes but typically not read)
- : Total size of the entire shared memory segment in bytes
- : Offset from the beginning of the segment to the first available free space for allocation
- : Handle/identifier for the dynamic shared memory control segment used for managing dynamic shared memory
- : Pointer to the ShmemIndex table, which maintains the directory of named shared memory allocations
- : (Unix/Linux only) Device identifier where the PostgreSQL data directory resides, used for validation
- : (Unix/Linux only) Inode number of the PostgreSQL data directory, used for validation

## Dependencies
- Functions called/Symbols referenced:
  - pid_t (process ID type)
  - dsm_handle (dynamic shared memory handle type)
- Called from (representative examples):
  - PGSharedMemoryCreate (creates and initializes the header)
  - PGSharedMemoryAttach (reads and validates the header)
  - PGSharedMemoryReAttach (reattaches using header information)
  - CreateSharedMemoryAndSemaphores (uses header during initialization)
  - InitShmemAccess (accesses header for shared memory setup)

## Notes and Other Information
- The magic number PGShmemMagic (679834894) is used as a signature to validate that a shared memory segment belongs to PostgreSQL
- The structure layout is platform-dependent due to conditional compilation for Windows vs. Unix-like systems
- On Windows, device and inode fields are omitted since Windows doesn't provide useful inode numbers
- The header is typically located at the very beginning of each PostgreSQL shared memory segment
- This structure is fundamental to PostgreSQL's shared memory architecture and is used across both System V and POSIX shared memory implementations
- The freeoffset field enables dynamic allocation within the shared memory segment by tracking the boundary between allocated and free space