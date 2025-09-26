# RemoveGUCFromLists

## Location
[src/backend/utils/misc/guc.c:1763-1785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1763-L1785)

## Overview
RemoveGUCFromLists is a static helper function that summarily removes a GUC (Grand Unified Configuration) variable from any linked lists it participates in, used when the variable is about to be deleted or reset.

## Definition

```c
struct config_generic *gconf)
{
	if (gconf->source != PGC_S_DEFAULT)
		dlist_delete(&gconf->nondef_link);
	if (gconf->stack != NULL)
		slist_delete(&guc_stack_list, &gconf->stack_link);
	if (gconf->status & GUC_NEEDS_REPORT)
		slist_delete(&guc_report_list, &gconf->report_link);
}


/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success;
```
## Detailed Description
This function performs cleanup operations on a GUC variable by removing it from three possible linked lists maintained by the GUC system:

1. **Non-default values list**: If the GUC has a non-default source, it's removed from the non-default configuration list
2. **Stack list**: If the GUC has stacked values (from nested transactions or function calls), it's removed from the global stack list
3. **Report list**: If the GUC is marked as needing to be reported to clients, it's removed from the report list

The function is designed to be called in uncommon operations like variable deletion or reset, so performance is not critical. It ensures that all references to the GUC variable are properly cleaned up from the various tracking lists.

## Parameters
- `gconf`: Pointer to the config_generic structure representing the GUC variable to be removed from lists

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md) (struct)
  - PGC_S_DEFAULT (constant)
  - [dlist_delete](../d/dlist_delete.md) (function)
  - [slist_delete](../s/slist_delete.md) (function)
  - GUC_NEEDS_REPORT (flag)
- Called from (representative examples):
  - [define_custom_variable](../d/define_custom_variable.md)
  - [MarkGUCPrefixReserved](../M/MarkGUCPrefixReserved.md)
  - [RestoreGUCState](RestoreGUCState.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the guc.c file
- The function handles three different list types: doubly-linked lists (dlist) for non-default values, and singly-linked lists (slist) for stack and report tracking
- The function performs conditional removals based on the current state of the GUC variable (source, stack presence, and report flag)
- Since deletion/reset operations are uncommon, the function prioritizes correctness over performance
- The function is part of the cleanup mechanism for GUC variables and helps prevent memory leaks and dangling references