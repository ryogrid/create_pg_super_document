# ResetAllOptions

## Location
[src/backend/utils/misc/guc.c:2005-2112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2005-L2112)

## Overview
ResetAllOptions is a function that implements the SQL RESET ALL command by resetting all eligible GUC (Grand Unified Configuration) parameters to their default values.

## Definition


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
- None (void function)

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