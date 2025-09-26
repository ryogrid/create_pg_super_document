# ShmemIndexEnt

## Location
[src/include/storage/shmem.h:57-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/shmem.h#L57-L59)

## Overview
ShmemIndexEnt is a structure that represents a hash bucket entry in PostgreSQL's shared memory index table, used to track and manage named shared memory segments.

## Definition

```c
structure */
	Size		allocated_size;
```
## Detailed Description
ShmemIndexEnt serves as an index entry in PostgreSQL's shared memory management system. It acts as a hash table bucket that maintains metadata about allocated shared memory segments. Each entry maps a string identifier to a specific location in shared memory, along with size information for proper memory management.

The structure is fundamental to PostgreSQL's shared memory architecture, enabling the database to efficiently locate and manage various shared data structures like buffer pools, lock tables, and other critical system components. The index allows processes to find shared memory segments by name rather than requiring hardcoded memory addresses.

The key field uses a fixed-size buffer (48 bytes) to store the string name identifier, while the location pointer provides direct access to the actual shared memory segment. The dual size tracking (requested vs. allocated) helps with memory debugging and efficient allocation strategies.

## Parameters / Member Variables
- : A fixed-size character array (48 bytes) that stores the string identifier name for the shared memory segment
- : A void pointer that points to the actual location of the shared memory segment in the shared memory space
- : The number of bytes that were originally requested when allocating this shared memory structure
- : The actual number of bytes that were allocated for this structure, which may be larger than the requested size due to alignment requirements

## Dependencies
- Functions called/Symbols referenced:
  - SHMEM_INDEX_KEYSIZE (constant defining key array size as 48 bytes)
  - Size (PostgreSQL's standard size type)
  
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (in src/backend/storage/ipc/ipci.c:116)
  - [InitShmemIndex](../I/InitShmemIndex.md) (in src/backend/storage/ipc/shmem.c:296)
  - [ShmemInitStruct](ShmemInitStruct.md) (in src/backend/storage/ipc/shmem.c:389, 428)
  - PG_GET_SHMEM_SIZES_COLS (in src/backend/storage/ipc/shmem.c:532, 545)

## Notes and Other Information
- The structure is defined in src/include/storage/shmem.h:51-57
- As a typedef struct, this is a data type definition rather than a function, so it doesn't have references from itself to other symbols
- The fixed 48-byte key size (SHMEM_INDEX_KEYSIZE) provides a reasonable balance between memory usage and identifier length flexibility
- The distinction between requested and allocated sizes is important for memory management debugging and helps track fragmentation or alignment-related overhead
- This structure is central to PostgreSQL's shared memory management and is used extensively throughout the backend for organizing shared data structures