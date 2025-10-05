# can_skip_gucvar

## Location
[src/backend/utils/misc/guc.c:5822-5855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5822-L5855)

## Overview
Determines whether SerializeGUCState can skip sending a GUC variable or whether RestoreGUCState can skip resetting a GUC to default during parallel worker communication.

## Definition

```c
static bool
can_skip_gucvar(struct config_generic *gconf)
```
## Detailed Description
This function implements optimization logic for GUC (Grand Unified Configuration) variable serialization and restoration in parallel query execution. It decides which GUC variables can be safely omitted during the leader-to-worker communication process.

The function uses a "magical" test that works for both serialization (leader side) and restoration (worker side) scenarios:
- On the leader side: Skip sending GUCs that workers are guaranteed to already have correctly set
- On the worker side: Skip resetting GUCs that already have their default values

The optimization is based on the principle that certain GUCs are guaranteed to have the same values in leaders and workers, eliminating unnecessary data transfer and processing.

## Parameters / Member Variables
- `*gconf`: Pointer to a config_generic structure representing the GUC variable to evaluate
## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](config_generic.md) (struct type)
  - PGC_POSTMASTER (enum constant)
  - PGC_INTERNAL (enum constant)  
  - PGC_S_DEFAULT (enum constant)
- Called from (representative examples):
  - [estimate_variable_size](../e/estimate_variable_size.md)
  - [serialize_variable](../s/serialize_variable.md)
  - [RestoreGUCState](../R/RestoreGUCState.md)

## Notes and Other Information
- The same test logic works for both leader and worker sides, but may select different sets of GUCs on each side
- PGC_POSTMASTER variables are always skipped because they have the same value in every child process
- PGC_INTERNAL variables are always skipped because they're set by special mechanisms
- Other GUCs are skipped only if they have their compiled-in default value (source == PGC_S_DEFAULT)
- This optimization typically saves significant work by avoiding transmission of default-valued GUCs
- The function is critical for parallel query performance and correctness

## Simplified Source

```c
static bool can_skip_gucvar(struct config_generic *gconf)
{
    // Skip GUCs that are guaranteed to have same values in leaders and workers

    // Always skip POSTMASTER vars (same in all children)
    // Always skip INTERNAL vars (set by special mechanisms)
    // Skip other GUCs if they have default value
    return gconf->context == PGC_POSTMASTER ||
           gconf->context == PGC_INTERNAL ||
           gconf->source == PGC_S_DEFAULT;
}
```