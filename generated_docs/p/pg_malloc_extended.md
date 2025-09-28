# pg_malloc_extended

## Location
[src/common/fe_memutils.c:59-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L59-L64)

## Overview
Extended memory allocation function that provides configurable allocation behavior through flags for zero-initialization and error handling.

## Definition

```c
void *
pg_malloc_extended(size_t size, int flags)
```
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
  - [pg_malloc_internal](pg_malloc_internal.md) (passes flags directly)
- Called from (representative examples):
  - [GetPrivilegesToDelete](../G/GetPrivilegesToDelete.md) (pg_ctl)
  - [Zstd_open](../Z/Zstd_open.md) (compress_zstd)
  - [do_lo_import](../d/do_lo_import.md) (psql)
  - [pg_log_generic_v](pg_log_generic_v.md) (logging)

## Notes and Other Information
- This is the most flexible memory allocation function in PostgreSQL's frontend utilities
- Allows combining flags: 
- With MCXT_ALLOC_NO_OOM flag, callers must check for NULL return value
- Without MCXT_ALLOC_NO_OOM flag, behavior is identical to pg_malloc() or pg_malloc0() depending on MCXT_ALLOC_ZERO flag
- Provides the foundation for other allocation functions: pg_malloc() calls this with flags=0, pg_malloc0() calls this with flags=MCXT_ALLOC_ZERO
- Located in src/common/fe_memutils.c:59-64

## Simplified Source

```c
// Simplified version of pg_malloc_extended
void *pg_malloc_extended(size_t size, int flags) {
    // Direct wrapper - delegate to internal allocation function
    return pg_malloc_internal(size, flags);
}

// The actual allocation logic (from pg_malloc_internal):
static inline void *pg_malloc_internal(size_t size, int flags) {
    // Ensure non-zero allocation size for portability
    if (size == 0) size = 1;

    // Attempt memory allocation
    void *memory = malloc(size);

    // Handle allocation failure based on flags
    if (memory == NULL) {
        if (!(flags & MCXT_ALLOC_NO_OOM)) {
            // Exit program on failure (default behavior)
            fprintf(stderr, "out of memory\n");
            exit(EXIT_FAILURE);
        }
        return NULL;  // Return NULL if NO_OOM flag set
    }

    // Zero-initialize if requested
    if (flags & MCXT_ALLOC_ZERO) {
        memset(memory, 0, size);
    }

    return memory;
}
```

Key simplifications made:
- Combined the wrapper function with its internal implementation for clarity
- Replaced `MemSet` macro with standard `memset` for readability
- Simplified flag checking logic with clearer condition expressions
- Added descriptive comments explaining each major step
- Removed `tmp` variable in favor of direct `memory` variable name
- Consolidated error handling flow into clearer conditional structure