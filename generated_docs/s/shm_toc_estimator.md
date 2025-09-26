# shm_toc_estimator

## Location
[src/include/storage/shm_toc.h:47-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/shm_toc.h#L47-L48)

## Overview
A utility structure for estimating the total size of shared memory needed for a shared memory table of contents (TOC) and its associated data chunks before allocation.

## Definition
```c
typedef struct
{
    Size        space_for_chunks;
    Size        number_of_keys;
} shm_toc_estimator;
```

## Detailed Description
The `shm_toc_estimator` structure is a planning tool used to calculate the total amount of shared memory required for a complete shared memory table of contents system. This includes both the TOC metadata structure itself and all the data chunks that will be stored within the shared memory segment.

The estimator works by accumulating size requirements for:
1. Individual data chunks that will be stored in the shared memory segment
2. The number of TOC entries (keys) that will be needed to reference these chunks

The estimation process is completed by calling `shm_toc_estimate()` which combines both metrics to compute the total memory requirement, including proper alignment and overhead for the TOC structure itself.

This estimation approach allows PostgreSQL to pre-calculate the exact size of dynamic shared memory segments before allocation, preventing memory exhaustion and ensuring optimal memory layout.

## Parameters / Member Variables
- `space_for_chunks`: Total bytes needed for all data chunks that will be stored in the shared memory segment (accumulated via `shm_toc_estimate_chunk()`)
- `number_of_keys`: Total number of TOC entries (keys) that will be needed to reference the data chunks (accumulated via `shm_toc_estimate_keys()`)

## Dependencies
- Functions called/Symbols referenced: None (this is a simple data structure)
- Used with helper macros:
  - `shm_toc_initialize_estimator()` - [Initialize](../I/Initialize.md) estimator to zero values
  - `shm_toc_estimate_chunk()` - Add chunk size to space estimation
  - `shm_toc_estimate_keys()` - Add number of keys to key count
- Used by function:
  - `[shm_toc_estimate](shm_toc_estimate.md)()` - Calculate final memory requirement from estimator

## Notes and Other Information
- The estimator is designed for use during the planning phase before shared memory allocation
- All size calculations use PostgreSQL's `add_size()` function to prevent integer overflow
- The `space_for_chunks` field automatically applies `BUFFERALIGN()` to ensure proper memory alignment
- This is not intended for large numbers of keys and will perform poorly if used that way
- The structure is typically initialized using `shm_toc_initialize_estimator()` macro
- Common usage pattern: initialize → estimate chunks → estimate keys → call `shm_toc_estimate()` → allocate shared memory
- Used extensively in parallel query processing, session management, and other subsystems that require dynamic shared memory