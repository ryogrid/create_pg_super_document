# parallel_vacuum_index_is_parallel_safe

## Location
[src/backend/commands/vacuumparallel.c:949-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L949-L986)

## Overview
Determines whether a given index can safely participate in parallel vacuum operations by checking the index's parallel vacuum support capabilities.

## Definition

```c
static bool
parallel_vacuum_index_is_parallel_safe(Relation indrel, int num_index_scans,
									   bool vacuum)
```
## Detailed Description
This function evaluates whether an index is eligible for parallel vacuum processing based on its access method's parallel vacuum capabilities. It performs different checks depending on whether the operation is bulk deletion (vacuum=true) or cleanup (vacuum=false).

The function examines the  field of the index's access method to determine supported parallel operations:

**For Bulk Deletion Operations (vacuum=true)**:
- Checks if the index supports 

**For Cleanup Operations (vacuum=false)**:
- Requires either  or 
- For conditional cleanup support, returns false if the index has already been processed (num_index_scans > 0)

The conditional cleanup logic prevents unnecessary worker invocation when parallel cleanup doesn't need to scan the index after bulk deletion has already been performed.

## Parameters / Member Variables
- `indrel`: The index relation to check for parallel safety
- `num_index_scans`: Number of times this index has been processed (used for conditional cleanup logic)
- `vacuum`: Boolean flag indicating the operation type (true for bulk deletion, false for cleanup)
## Dependencies
- Functions called/Symbols referenced:
  - VACUUM_OPTION_PARALLEL_BULKDEL
  - VACUUM_OPTION_PARALLEL_CLEANUP  
  - VACUUM_OPTION_PARALLEL_COND_CLEANUP
- Called from (representative examples):
  - [parallel_vacuum_process_all_indexes](parallel_vacuum_process_all_indexes.md)

## Notes and Other Information
- This is a static function used internally within the parallel vacuum implementation
- The function is essential for maintaining correctness in parallel vacuum by ensuring only compatible indexes participate
- Conditional cleanup support allows access methods to optimize when parallel cleanup is beneficial
- The num_index_scans parameter prevents redundant parallel cleanup operations on indexes that have already been processed
- Access method developers can use different vacuum option flags to control parallel vacuum behavior

## Simplified Source

```c
static bool
parallel_vacuum_index_is_parallel_safe(Relation indrel, int num_index_scans,
                                       bool vacuum)
{
    uint8 vacoptions = indrel->rd_indam->amparallelvacuumoptions;

    // For bulk deletion, check if parallel bulk-delete is supported
    if (vacuum)
        return ((vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0);

    // For cleanup operations, check parallel cleanup support
    if (((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0) &&
        ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0))
        return false;

    // Skip conditional cleanup if index already processed
    if (num_index_scans > 0 &&
        ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0))
        return false;

    return true;
}
```