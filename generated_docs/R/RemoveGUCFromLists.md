# RemoveGUCFromLists

## Location
src/backend/utils/misc/guc.c: 1763 - 1785

## Overview
RemoveGUCFromLists is a static helper function that summarily removes a GUC (Grand Unified Configuration) variable from any linked lists it participates in, used when the variable is about to be deleted or reset.

## Definition


## Detailed Description
This function performs cleanup operations on a GUC variable by removing it from three possible linked lists maintained by the GUC system:

1. **Non-default values list**: If the GUC has a non-default source, it's removed from the non-default configuration list
2. **Stack list**: If the GUC has stacked values (from nested transactions or function calls), it's removed from the global stack list
3. **Report list**: If the GUC is marked as needing to be reported to clients, it's removed from the report list

The function is designed to be called in uncommon operations like variable deletion or reset, so performance is not critical. It ensures that all references to the GUC variable are properly cleaned up from the various tracking lists.

## Parameters / Member Variables
- : Pointer to the config_generic structure representing the GUC variable to be removed from lists

## Dependencies
- Functions called/Symbols referenced:
  - config_generic (struct)
  - PGC_S_DEFAULT (constant)
  - dlist_delete (function)
  - slist_delete (function)
  - GUC_NEEDS_REPORT (flag)
- Called from (representative examples):
  - define_custom_variable
  - MarkGUCPrefixReserved
  - RestoreGUCState

## Notes and Other Information
- This is a static function, meaning it's only accessible within the guc.c file
- The function handles three different list types: doubly-linked lists (dlist) for non-default values, and singly-linked lists (slist) for stack and report tracking
- The function performs conditional removals based on the current state of the GUC variable (source, stack presence, and report flag)
- Since deletion/reset operations are uncommon, the function prioritizes correctness over performance
- The function is part of the cleanup mechanism for GUC variables and helps prevent memory leaks and dangling references