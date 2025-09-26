# pg_malloc0

## Location
src/common/fe_memutils.c: 53 - 58

## Overview
Memory allocation function that allocates zero-initialized memory with out-of-memory error handling.

## Definition


## Detailed Description
pg_malloc0 is a wrapper function around pg_malloc_internal that provides memory allocation with automatic zero-initialization. It allocates the requested amount of memory and initializes all bytes to zero before returning the pointer. Like pg_malloc, it exits the program with an error message if allocation fails.

This function is the PostgreSQL equivalent of the standard C library calloc(1, size) function but with added safety features and consistent behavior across PostgreSQL frontend applications. It's commonly used when you need clean, initialized memory for structures, arrays, or buffers.

## Parameters / Member Variables
- : The number of bytes to allocate and zero-initialize

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_internal (with MCXT_ALLOC_ZERO flag)
- Called from (representative examples):
  - compile_database_list (pg_amcheck)
  - StartLogStreamer (pg_basebackup)
  - GetConnection (streamutil)
  - load_backup_manifest (pg_combinebackup)
  - AllocateCompressor (compress_io)
  - NewRestoreOptions (pg_backup_archiver)
  - setup_connection (pg_dump)
  - parallel_exec_prog (pg_upgrade)
  - printTableInit (fe_utils)

## Notes and Other Information
- Provides zero-initialized memory, equivalent to calloc(1, size) but with PostgreSQL's error handling
- Always terminates the program on allocation failure (no graceful error handling)
- Uses PostgreSQL's MemSet macro for zero-initialization rather than relying on calloc()
- Handles malloc(0) portability issues by ensuring at least 1 byte is allocated
- For applications that need to handle allocation failures gracefully, use pg_malloc_extended() with both MCXT_ALLOC_ZERO and MCXT_ALLOC_NO_OOM flags
- Located in src/common/fe_memutils.c:53-58