# ResetAllOptions

## Location
[src/backend/utils/misc/guc.c:2005-2112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2005-L2112)

## Overview
ResetAllOptions is a function that implements the SQL RESET ALL command by resetting all eligible GUC (Grand Unified Configuration) parameters to their default values.

## Definition

```c
struct config_generic *gconf = dlist_container(struct config_generic,
													   nondef_link, iter.cur);
```
## Detailed Description
This function systematically resets all configuration parameters that are eligible for the RESET ALL operation. It iterates through the list of non-default GUC variables and selectively resets them based on several criteria:

**Eligibility Criteria:**
1. The parameter must be settable by users (PGC_SUSET or PGC_USERSET context)
2. The parameter must not have the GUC_NO_RESET_ALL flag set
3. The parameter's source must be higher than PGC_S_OVERRIDE (meaning it was explicitly SET)

**Reset Process:**
1. **Transaction Safety**: Saves the old value using push_old_value to support transaction rollback
2. **Type-Specific Reset**: Handles different GUC types (bool, int, real, string, enum) appropriately
3. **Hook Execution**: Calls assign_hook functions if present to perform side effects
4. **Value Assignment**: Sets the variable to its reset_val
5. **Extra Data**: Updates any extra data associated with the parameter
6. **Source Management**: Updates the source, context, and role information
7. **Reporting**: Adds the parameter to the report list if it needs to be reported to clients

The function maintains transaction integrity by preserving old values on a stack, allowing for proper rollback behavior.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify, dlist_container
  - [push_old_value](../p/push_old_value.md)
  - [set_extra_field](../s/set_extra_field.md), set_string_field
  - [set_guc_source](../s/set_guc_source.md)
  - [slist_push_head](../s/slist_push_head.md)
  - Various GUC constants (PGC_SUSET, PGC_USERSET, GUC_NO_RESET_ALL, etc.)
  - Configuration structures (config_bool, config_int, config_real, config_string, config_enum)
- Called from (representative examples):
  - [DiscardAll](../D/DiscardAll.md)
  - [ExecSetVariableStmt](../E/ExecSetVariableStmt.md)

## Notes and Other Information
- This function implements the backend for the SQL "RESET ALL" statement
- The function respects GUC context levels - it won't reset parameters that users aren't allowed to modify
- Some parameters are explicitly excluded from RESET ALL via the GUC_NO_RESET_ALL flag
- The function is transaction-aware and supports proper rollback behavior
- Only parameters that were explicitly SET (source > PGC_S_OVERRIDE) are candidates for reset
- The function handles all GUC data types through a type-specific switch statement
- After resetting, parameters that need client notification are added to the report list
- The iteration uses dlist_foreach_modify to safely modify the list while iterating

## Simplified Source

```c
void
ResetAllOptions(void)
{
    dlist_mutable_iter iter;

    // Iterate through all non-default GUC variables
    dlist_foreach_modify(iter, &guc_nondef_list) {
        struct config_generic *gconf = dlist_container(struct config_generic,
                                                       nondef_link, iter.cur);

        // Skip non-user-settable parameters
        if (gconf->context != PGC_SUSET && gconf->context != PGC_USERSET)
            continue;

        // Skip parameters excluded from RESET ALL
        if (gconf->flags & GUC_NO_RESET_ALL)
            continue;

        // Skip parameters that weren't explicitly SET
        if (gconf->source <= PGC_S_OVERRIDE)
            continue;

        // Save old value for transaction rollback support
        push_old_value(gconf, GUC_ACTION_SET);

        // Reset based on parameter type
        switch (gconf->vartype) {
            case PGC_BOOL: {
                struct config_bool *conf = (struct config_bool *) gconf;
                if (conf->assign_hook)
                    conf->assign_hook(conf->reset_val, conf->reset_extra);
                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
            case PGC_INT: {
                struct config_int *conf = (struct config_int *) gconf;
                if (conf->assign_hook)
                    conf->assign_hook(conf->reset_val, conf->reset_extra);
                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
            case PGC_REAL: {
                struct config_real *conf = (struct config_real *) gconf;
                if (conf->assign_hook)
                    conf->assign_hook(conf->reset_val, conf->reset_extra);
                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
            case PGC_STRING: {
                struct config_string *conf = (struct config_string *) gconf;
                if (conf->assign_hook)
                    conf->assign_hook(conf->reset_val, conf->reset_extra);
                set_string_field(conf, conf->variable, conf->reset_val);
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
            case PGC_ENUM: {
                struct config_enum *conf = (struct config_enum *) gconf;
                if (conf->assign_hook)
                    conf->assign_hook(conf->reset_val, conf->reset_extra);
                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
        }

        // Update source and context information
        set_guc_source(gconf, gconf->reset_source);
        gconf->scontext = gconf->reset_scontext;
        gconf->srole = gconf->reset_srole;

        // Add to report list if needed
        if ((gconf->flags & GUC_REPORT) && !(gconf->status & GUC_NEEDS_REPORT)) {
            gconf->status |= GUC_NEEDS_REPORT;
            slist_push_head(&guc_report_list, &gconf->report_link);
        }
    }
}
```