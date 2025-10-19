# pg_malloc0

## Location
[src/common/fe_memutils.c:53-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L53-L58)

## Overview
Memory allocation function that allocates zero-initialized memory with out-of-memory error handling.

## Definition

```c
void *
pg_malloc0(size_t size)
```
## Detailed Description
pg_malloc0 is a wrapper function around pg_malloc_internal that provides memory allocation with automatic zero-initialization. It allocates the requested amount of memory and initializes all bytes to zero before returning the pointer. Like pg_malloc, it exits the program with an error message if allocation fails.

This function is the PostgreSQL equivalent of the standard C library calloc(1, size) function but with added safety features and consistent behavior across PostgreSQL frontend applications. It's commonly used when you need clean, initialized memory for structures, arrays, or buffers.

## Parameters / Member Variables
- `size`: The number of bytes to allocate and zero-initialize
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc_internal](pg_malloc_internal.md) (with MCXT_ALLOC_ZERO flag)
- Called from (representative examples):
  - [compile_database_list](../c/compile_database_list.md) (pg_amcheck)
  - [StartLogStreamer](../S/StartLogStreamer.md) (pg_basebackup)
  - [GetConnection](../G/GetConnection.md) (streamutil)
  - [load_backup_manifest](../l/load_backup_manifest.md) (pg_combinebackup)
  - [AllocateCompressor](../A/AllocateCompressor.md) (compress_io)
  - [NewRestoreOptions](../N/NewRestoreOptions.md) (pg_backup_archiver)
  - [setup_connection](../s/setup_connection.md) (pg_dump)
  - [parallel_exec_prog](parallel_exec_prog.md) (pg_upgrade)
  - [printTableInit](printTableInit.md) (fe_utils)

## Notes and Other Information
- Provides zero-initialized memory, equivalent to calloc(1, size) but with PostgreSQL's error handling
- Always terminates the program on allocation failure (no graceful error handling)
- Uses PostgreSQL's MemSet macro for zero-initialization rather than relying on calloc()
- Handles malloc(0) portability issues by ensuring at least 1 byte is allocated
- For applications that need to handle allocation failures gracefully, use pg_malloc_extended() with both MCXT_ALLOC_ZERO and MCXT_ALLOC_NO_OOM flags
- Located in src/common/fe_memutils.c:53-58

## Simplified Source

```c
void *pg_malloc0(size_t size) {
    // Allocate memory with zero-initialization flag
    return pg_malloc_internal(size, MCXT_ALLOC_ZERO);
}
```