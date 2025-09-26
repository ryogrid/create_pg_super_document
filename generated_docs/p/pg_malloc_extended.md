# pg_malloc_extended

## Location
src/common/fe_memutils.c: 59 - 64

## Overview
Extended memory allocation function that provides configurable allocation behavior through flags for zero-initialization and error handling.

## Definition


## Detailed Description
pg_malloc_extended is a direct wrapper around pg_malloc_internal that exposes the full flexibility of PostgreSQL's frontend memory allocation system. It allows callers to specify exact allocation behavior through flags, making it suitable for cases where the standard pg_malloc() or pg_malloc0() functions are too restrictive.

This function provides the most control over memory allocation behavior, allowing applications to choose whether to zero-initialize memory and whether to handle allocation failures gracefully or terminate the program.

## Parameters / Member Variables
- : The number of bytes to allocate
- : Control flags that modify allocation behavior:
  - : If set, returns NULL on allocation failure instead of exiting
  - : If set, initializes the allocated memory to zero
  - Flags can be combined using bitwise OR operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_internal (passes flags directly)
- Called from (representative examples):
  - GetPrivilegesToDelete (pg_ctl)
  - Zstd_open (compress_zstd)
  - do_lo_import (psql)
  - pg_log_generic_v (logging)

## Notes and Other Information
- This is the most flexible memory allocation function in PostgreSQL's frontend utilities
- Allows combining flags: 
- With MCXT_ALLOC_NO_OOM flag, callers must check for NULL return value
- Without MCXT_ALLOC_NO_OOM flag, behavior is identical to pg_malloc() or pg_malloc0() depending on MCXT_ALLOC_ZERO flag
- Provides the foundation for other allocation functions: pg_malloc() calls this with flags=0, pg_malloc0() calls this with flags=MCXT_ALLOC_ZERO
- Located in src/common/fe_memutils.c:59-64