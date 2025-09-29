# set_guc_source

## Location
[src/backend/utils/misc/guc.c:2113-2135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2113-L2135)

## Overview
set_guc_source is a static helper function that safely updates a GUC variable's source field while maintaining the integrity of the guc_nondef_list.

## Definition

```c
static void
set_guc_source(struct config_generic *gconf, GucSource newsource)
```
## Detailed Description
This function provides a controlled way to update a GUC parameter's source field while ensuring that the parameter's membership in the guc_nondef_list (list of non-default GUC variables) is properly maintained. The function handles the transitions between default and non-default sources:

**Key Operations:**
1. **List Membership Management**: Automatically adds or removes the GUC from guc_nondef_list based on source transitions
2. **Default to Non-Default**: When a parameter changes from PGC_S_DEFAULT to any other source, it's added to guc_nondef_list
3. **Non-Default to Default**: When a parameter changes from any source to PGC_S_DEFAULT, it's removed from guc_nondef_list
4. **Source Update**: Finally updates the actual source field

This approach ensures that the guc_nondef_list always accurately reflects which parameters have non-default values, which is critical for operations like configuration dumps, resets, and other GUC management functions.

## Parameters / Member Variables
- : Pointer to the config_generic structure representing the GUC variable
- : The new GucSource value to be assigned to the parameter's source field

## Dependencies
- Functions called/Symbols referenced:
  - GucSource (enum type)
  - [config_generic](../c/config_generic.md) (struct)
  - PGC_S_DEFAULT (source constant)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [dlist_delete](../d/dlist_delete.md)
- Called from (representative examples):
  - [ResetAllOptions](../R/ResetAllOptions.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - Various GUC assignment functions

## Notes and Other Information
- This is a static function, only accessible within guc.c
- The function is essential for maintaining the consistency of GUC data structures
- Direct assignment to gconf->source should be avoided in favor of using this function
- The guc_nondef_list is used by various GUC operations to efficiently identify parameters that have been modified from their defaults
- The function handles both directions of source changes (default ↔ non-default) safely
- This abstraction prevents bugs that could occur from forgetting to update list membership when changing source values
- The function is part of the GUC system's internal bookkeeping and helps optimize operations that need to work with only non-default parameters

## Simplified Source

```c
static void
set_guc_source(struct config_generic *gconf, GucSource newsource)
{
    // Adjust guc_nondef_list membership based on source change
    if (gconf->source == PGC_S_DEFAULT)
    {
        if (newsource != PGC_S_DEFAULT)
            dlist_push_tail(&guc_nondef_list, &gconf->nondef_link);
    }
    else
    {
        if (newsource == PGC_S_DEFAULT)
            dlist_delete(&gconf->nondef_link);
    }

    // Update the source field
    gconf->source = newsource;
}
```