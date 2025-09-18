# dshash_table

## Location
src/backend/lib/dshash.c: 103 - 113

## Overview
The dshash_table struct represents per-backend state for a dynamic shared hash table, providing local access and caching for efficient operations on shared hash table data.

## Definition
```c
struct dshash_table
{
    dsa_area                *area;      /* Backing dynamic shared memory area. */
    dshash_parameters       params;     /* Parameters. */
    void                    *arg;       /* User-supplied data pointer. */
    dshash_table_control    *control;   /* Control object in DSM. */
    dsa_pointer             *buckets;   /* Current bucket pointers in DSM. */
    size_t                  size_log2;  /* log2(number of buckets) */
};
```

## Detailed Description
The dshash_table struct serves as the per-backend interface to PostgreSQL's dynamic shared hash table system. Each backend process that needs to access a shared hash table maintains its own dshash_table instance that references the shared control structure and bucket array. This design separates per-process state (like cached bucket pointers and parameters) from the shared table metadata, enabling efficient local operations while maintaining consistency across processes. The struct caches frequently accessed information to minimize shared memory access and provides the necessary context for hash table operations.

## Parameters / Member Variables
- `area`: A pointer to the dsa_area that manages the dynamic shared memory segment containing the hash table
- `params`: A dshash_parameters structure containing configuration settings like hash and comparison functions
- `arg`: A void pointer to user-supplied data that is passed to callback functions (hash, compare, etc.)
- `control`: A pointer to the dshash_table_control structure in shared memory that coordinates table operations
- `buckets`: A cached pointer to the current bucket array in dynamic shared memory for fast local access
- `size_log2`: A cached copy of the table size (log base 2) to avoid accessing shared memory for bucket calculations

## Dependencies
- Functions called/Symbols referenced:
  - dsa_area
  - dshash_parameters  
  - dshash_table_control
  - dsa_pointer
- Called from (representative examples):
  - dshash_create
  - dshash_attach
  - dshash_detach
  - dshash_destroy
  - dshash_find
  - dshash_find_or_insert
  - dshash_delete_key
  - dshash_delete_entry
  - BUCKET_FOR_HASH (macro)
  - find_in_bucket
  - insert_into_bucket

## Notes and Other Information
- Each backend process has its own dshash_table instance even when accessing the same shared hash table
- The buckets and size_log2 fields are cached locally to avoid frequent shared memory access during bucket lookups
- The structure provides the local context needed for all hash table operations while referencing shared data
- This design pattern allows PostgreSQL to efficiently share hash tables across multiple backend processes
- The arg field enables user-defined callback functions to access additional context during hash table operations
- The structure is part of PostgreSQL's dynamic shared area (DSA) infrastructure for inter-process data sharing