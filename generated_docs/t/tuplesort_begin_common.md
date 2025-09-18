# tuplesort_begin_common

## Location
src/backend/utils/sort/tuplesort.c: 645 - 756

## Overview
The core initialization function for PostgreSQL's tuple sorting system that sets up the common infrastructure for all tuplesort operations, including memory management, parallel coordination, and basic state initialization.

## Definition
```c
Tuplesortstate *tuplesort_begin_common(int workMem, SortCoordinate coordinate, int sortopt)
```

## Detailed Description
This function serves as the foundation for all tuplesort operations in PostgreSQL. It creates and initializes a `Tuplesortstate` structure with the necessary memory contexts, configuration settings, and parallel processing coordination. The function establishes two memory contexts: a main context that survives across multiple sort batches and a sort context that gets reset between operations.

The function handles both serial and parallel sorting scenarios by examining the `coordinate` parameter. For parallel sorts, it sets up appropriate worker identification and shared state management. It enforces a minimum work memory of 64KB to protect against parallel workers with insufficient memory allocation.

After setting up the basic state, it calls `tuplesort_begin_batch` to initialize batch-specific components. The function also includes trace support for debugging sort operations when `TRACE_SORT` is enabled.

## Parameters / Member Variables
- `workMem`: Maximum kilobytes of RAM to use before spilling to disk (minimum 64KB enforced)
- `coordinate`: Parallel sort coordination information (NULL for serial sorts)
- `sortopt`: Bitmask of sort options (TUPLESORT_* flags from tuplesort.h)

## Dependencies
- Functions called/Symbols referenced:
  - `AllocSetContextCreate` - Creates memory contexts for sort operations
  - `tuplesort_begin_batch` - Initializes batch-specific state
  - `worker_get_identifier` - Gets worker ID for parallel sorts
  - `pg_rusage_init` - Initializes resource usage tracking (if TRACE_SORT enabled)
  - `TUPLESORT_RANDOMACCESS` - Sort option constant
  - `INITIAL_MEMTUPSIZE` - Initial memory tuple array size
- Called from (representative examples):
  - `tuplesort_begin_heap` - For heap tuple sorting
  - `tuplesort_begin_cluster` - For cluster operations
  - `tuplesort_begin_index_btree` - For B-tree index creation
  - `tuplesort_begin_index_hash` - For hash index creation
  - `tuplesort_begin_datum` - For single datum sorting

## Notes and Other Information
- Serves as the common foundation for all specialized tuplesort variants
- Creates persistent memory contexts that survive across multiple sort batches
- Enforces minimum work memory of 64KB to protect parallel workers
- Validates that random access is not requested for parallel sorts
- Sets up infrastructure for both serial and parallel sorting scenarios
- The returned `Tuplesortstate` must be freed with `tuplesort_end`
- Memory context management ensures proper cleanup and isolation between sort operations
- Part of PostgreSQL's sophisticated memory management and parallel processing architecture