# RestoreGUCState

## Location
src/backend/utils/misc/guc.c: 6201 - 6369

## Overview
RestoreGUCState reads GUC (Grand Unified Configuration) state from a serialized address and sets the current process's GUCs to match, primarily used for parallel worker processes to inherit configuration from their leader.

## Definition


## Detailed Description
RestoreGUCState provides parallel worker processes with a shallow view of the leader's GUC state by deserializing and applying configuration values. The function operates in two main phases:

1. **Reset Phase**: First ensures that all potentially-shippable GUCs are reset to their default values using the same criteria as SerializeGUCState. This handles cases where the worker may have different initial settings than the leader.

2. **Restore Phase**: Deserializes the GUC data from the provided memory buffer and applies each configuration using set_config_option_ext, including source file information when available.

The function provides only active GUC values to workers, not stacked or reset values, which is sufficient since workers execute within a single query context where active values don't change and stacked values are invisible.

## Parameters / Member Variables
- : Pointer to serialized GUC state data created by SerializeGUCState, containing configuration variable names, values, sources, and contexts.

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify (iterate through guc_nondef_list)
  - can_skip_gucvar (check if GUC should be skipped)
  - guc_free (free GUC memory allocations)
  - RemoveGUCFromLists (remove GUC from tracking lists)
  - InitializeOneGUCOption (reset GUC to default state)
  - read_gucstate/read_gucstate_binary (deserialize GUC data)
  - set_config_option_ext (apply GUC values)
  - set_config_sourcefile (set source file information)
- Called from (representative examples):
  - ParallelWorkerMain (src/backend/access/transam/parallel.c:1450)

## Notes and Other Information
- The function assumes the GUC stack is empty (Assert(gconf->stack == NULL))
- Uses error context callbacks to provide useful error messages during GUC restoration
- Memory management is carefully handled to avoid leaks when resetting GUCs
- Works in conjunction with SerializeGUCState for parallel query execution
- Only processes 'shippable' GUCs that can be safely transferred between processes