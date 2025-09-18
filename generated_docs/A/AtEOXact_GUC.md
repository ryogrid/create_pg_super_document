# AtEOXact_GUC

## Location
src/backend/utils/misc/guc.c: 2264 - 2547

## Overview
AtEOXact_GUC handles the complex cleanup and restoration of GUC (Grand Unified Configuration) variable states at transaction commit, abort, or when exiting nested GUC contexts.

## Definition


## Detailed Description
This function is the cornerstone of PostgreSQL's transactional GUC system, responsible for properly managing the stack of GUC variable states when exiting transaction contexts. Despite its name suggesting transaction-only usage, it handles various scenarios including subtransaction boundaries, function exits with proconfig settings, and transient GUC assignments. The function implements sophisticated logic for determining which values to restore based on the context (commit vs. abort) and the type of GUC operations performed.

Key behaviors:
- Processes all GUCs with non-empty stacks at or above the specified nest level
- For aborts: Always restores prior values to ensure consistent rollback
- For commits: Complex logic based on SET/SET LOCAL/SAVE states
- Handles stack merging when multiple nesting levels are involved
- Manages memory cleanup to prevent leaks
- Triggers reporting for GUCs that have the GUC_REPORT flag set
- Updates the global GUCNestLevel to reflect the new nesting state

## Parameters / Member Variables
- : Boolean indicating whether this is a commit (true) or abort (false) operation
- : The target nesting level to return to (1 = main transaction, higher values = subtransaction levels)

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach_modify, slist_container, slist_delete_current, slist_push_head (list management)
  - [discard_stack_value](../d/discard_stack_value.md), set_extra_field, set_string_field (value management)
  - [set_guc_source](../s/set_guc_source.md) (source tracking)
  - GUC state enums: GUC_SAVE, GUC_SET, GUC_LOCAL, GUC_SET_LOCAL
  - GUC type structs: config_bool, config_int, config_real, config_string, config_enum
  - GUC reporting flags: GUC_REPORT, GUC_NEEDS_REPORT
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md), AbortTransaction (main transaction boundaries)
  - [CommitSubTransaction](../C/CommitSubTransaction.md), AbortSubTransaction (subtransaction boundaries)  
  - [fmgr_security_definer](../f/fmgr_security_definer.md) (function execution cleanup)
  - Various DDL operations (index creation, schema operations, etc.)

## Notes and Other Information
- This is a public function declared in guc.h, essential for transaction management
- The function name is somewhat misleading - it handles more than just end-of-transaction scenarios
- Complex state machine logic handles interactions between SET and SET LOCAL operations
- Stack merging logic ensures proper value inheritance across nesting levels
- Memory management is critical - unused stack values must be properly discarded
- The function includes extensive assertions to detect improper nesting level usage
- Supports recovery from failures during transaction start before AtStart_GUC is called
- Central to PostgreSQL's ability to make configuration changes transactional and rollback-safe
- Works in conjunction with push_old_value, NewGUCNestLevel, and AtStart_GUC to provide complete GUC transaction support