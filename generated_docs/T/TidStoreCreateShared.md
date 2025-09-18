# TidStoreCreateShared

## Location
src/backend/access/common/tidstore.c: 213 - 254

## Overview
Creates a shared TidStore instance that can be accessed by multiple processes, using a Dynamic Shared Area (DSA) for the underlying TID storage while keeping metadata in backend-local memory.

## Definition
```c
TidStore *TidStoreCreateShared(size_t max_bytes, int tranche_id)
```

## Detailed Description
TidStoreCreateShared creates a TidStore designed for shared access across multiple backend processes. Unlike TidStoreCreateLocal, this function uses a Dynamic Shared Area (DSA) for the actual TID storage, allowing multiple processes to access the same TID data. The TidStore structure itself is allocated in backend-local memory, but the underlying radix tree data lives in shared memory.

The function creates an AllocSetContext for storing radix tree metadata locally, while the actual TID data is stored in the DSA area. It optimizes DSA segment sizes based on the max_bytes hint, setting both initial and maximum segment sizes to be no larger than 1/8 of max_bytes to ensure efficient shared memory usage.

## Parameters / Member Variables
- `max_bytes`: A hint for the maximum expected memory usage, used to optimize DSA segment sizes (not an enforced limit)
- `tranche_id`: Identifier for the lock tranche used to coordinate access to the shared DSA area

## Dependencies
- Functions called/Symbols referenced:
  - `palloc0`
  - `AllocSetContextCreate`
  - `dsa_create_ext`
  - `shared_ts_create`
  - `DSA_DEFAULT_INIT_SEGMENT_SIZE`
  - `DSA_MAX_SEGMENT_SIZE`
  - `DSA_MIN_SEGMENT_SIZE`
  - `ALLOCSET_SMALL_SIZES`
- Called from (representative examples):
  - `parallel_vacuum_init` (src/backend/commands/vacuumparallel.c:379)
  - `parallel_vacuum_reset_dead_items` (src/backend/commands/vacuumparallel.c:483)
  - `test_create` (src/test/modules/test_tidstore/test_tidstore.c:110)

## Notes and Other Information
- The returned TidStore object is allocated in backend-local memory, but the TID data lives in shared memory
- The rt_context only contains metadata about the radix tree, not the actual TID data
- DSA segment sizes are automatically adjusted to be no larger than 1/8 of max_bytes for optimal shared memory usage
- The tranche_id is used for lock coordination when multiple processes access the shared TidStore
- Primarily used in parallel vacuum operations where multiple workers need to share TID information