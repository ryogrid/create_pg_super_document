# AtEOXact_GUC

## Location
[src/backend/utils/misc/guc.c:2264-2547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2264-L2547)

## Overview
AtEOXact_GUC handles the complex cleanup and restoration of GUC (Grand Unified Configuration) variable states at transaction commit, abort, or when exiting nested GUC contexts.

## Definition

```c
struct config_generic *gconf = slist_container(struct config_generic,
													   stack_link, iter.cur);
```
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
- [Complex](../C/Complex.md) state machine logic handles interactions between SET and SET LOCAL operations
- Stack merging logic ensures proper value inheritance across nesting levels
- Memory management is critical - unused stack values must be properly discarded
- The function includes extensive assertions to detect improper nesting level usage
- Supports recovery from failures during transaction start before AtStart_GUC is called
- Central to PostgreSQL's ability to make configuration changes transactional and rollback-safe
- Works in conjunction with push_old_value, NewGUCNestLevel, and AtStart_GUC to provide complete GUC transaction support

## Simplified Source

```c
// Simplified version of AtEOXact_GUC
void AtEOXact_GUC(bool isCommit, int nestLevel) {
    slist_mutable_iter iter;

    // Validate nesting level is reasonable
    Assert(nestLevel > 0 &&
           (nestLevel <= GUCNestLevel ||
            (nestLevel == GUCNestLevel + 1 && !isCommit)));

    // Process each GUC that has a non-empty stack
    slist_foreach_modify(iter, &guc_stack_list) {
        struct config_generic *gconf = slist_container(struct config_generic,
                                                     stack_link, iter.cur);
        GucStack *stack;

        // Pop stack entries at or above the target nest level
        while ((stack = gconf->stack) != NULL &&
               stack->nest_level >= nestLevel) {
            GucStack *prev = stack->prev;
            bool restorePrior = false;
            bool restoreMasked = false;
            bool changed = false;

            // Determine what value to restore based on commit/abort and stack state
            if (!isCommit) {
                // Always restore prior value on abort
                restorePrior = true;
            } else if (stack->state == GUC_SAVE) {
                restorePrior = true;
            } else if (stack->nest_level == 1) {
                // Main transaction commit - handle SET/SET LOCAL differently
                if (stack->state == GUC_SET_LOCAL)
                    restoreMasked = true;
                else if (stack->state == GUC_SET)
                    discard_stack_value(gconf, &stack->prior);
                else // GUC_LOCAL
                    restorePrior = true;
            } else if (prev == NULL || prev->nest_level < stack->nest_level - 1) {
                // Decrement nesting level instead of popping
                stack->nest_level--;
                continue;
            } else {
                // Merge stack entry into previous level
                merge_stack_entries(stack, prev);
            }

            // Restore the determined value
            if (restorePrior || restoreMasked) {
                config_var_value newvalue;
                GucSource newsource;
                GucContext newscontext;
                Oid newsrole;

                if (restoreMasked) {
                    newvalue = stack->masked;
                    newsource = PGC_S_SESSION;
                    newscontext = stack->masked_scontext;
                    newsrole = stack->masked_srole;
                } else {
                    newvalue = stack->prior;
                    newsource = stack->source;
                    newscontext = stack->scontext;
                    newsrole = stack->srole;
                }

                // Apply the new value based on GUC type
                changed = apply_guc_value(gconf, newvalue);

                // Clean up stacked extra values
                set_extra_field(gconf, &(stack->prior.extra), NULL);
                set_extra_field(gconf, &(stack->masked.extra), NULL);

                // Update source information
                set_guc_source(gconf, newsource);
                gconf->scontext = newscontext;
                gconf->srole = newsrole;
            }

            // Pop the stack entry
            gconf->stack = prev;
            if (prev == NULL)
                slist_delete_current(&iter);
            pfree(stack);

            // Mark for reporting if value changed
            if (changed && (gconf->flags & GUC_REPORT) &&
                !(gconf->status & GUC_NEEDS_REPORT)) {
                gconf->status |= GUC_NEEDS_REPORT;
                slist_push_head(&guc_report_list, &gconf->report_link);
            }
        }
    }

    // Update global nesting level
    GUCNestLevel = nestLevel - 1;
}
```

Key simplifications made:
- Consolidated the complex stack merging logic into a conceptual `merge_stack_entries()` function
- Abstracted the type-specific value restoration into `apply_guc_value()` function
- Removed detailed switch statements for each GUC type (bool, int, real, string, enum)
- Simplified the nested conditional logic for determining restore behavior
- Kept the essential algorithm flow: iterate stacks → determine restore action → apply value → clean up
- Preserved memory management and reporting logic at a high level
- Maintained the core transactional semantics while reducing implementation details