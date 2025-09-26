# EstimateGUCStateSpace

## Location
[src/backend/utils/misc/guc.c:5956-5986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5956-L5986)

## Overview
Returns the total size needed to store the GUC state for the current process during parallel worker initialization.

## Definition
```c
Size EstimateGUCStateSpace(void)
```

## Detailed Description
This function calculates the total buffer space required to serialize all non-default GUC (Grand Unified Configuration) variables from the current process for transmission to parallel worker processes. It is used during parallel query setup to determine how much shared memory space needs to be allocated for GUC state transfer.

The function iterates through the global `guc_nondef_list` (a doubly-linked list containing all GUCs that have been changed from their default values) and sums up the space requirements for each variable. This optimization only processes GUCs that actually need to be transmitted, skipping default-valued variables.

The calculation includes:
- Space for storing the total data size (Size header)
- Space for each non-default GUC variable (names, values, metadata)
- Additional space for skippable GUCs that are filtered out during estimation

## Parameters / Member Variables
- None (void function)

## Dependencies  
- Functions called/Symbols referenced:
  - dlist_foreach, dlist_container, dlist_iter (doubly-linked list operations)
  - config_generic (struct type)
  - estimate_variable_size
  - add_size
  - guc_nondef_list (global variable)
- Called from (representative examples):
  - InitializeParallelDSM
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Only processes non-default GUCs from the guc_nondef_list for efficiency
- The size estimation includes a Size header to store the total data size
- Uses safe arithmetic via add_size() to prevent integer overflow
- This function is part of the parallel query infrastructure for sharing configuration state
- The estimated size is used by InitializeParallelDSM to allocate appropriate shared memory
- Actual serialization is performed later by SerializeGUCState using this size estimate