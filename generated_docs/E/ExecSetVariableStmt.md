# ExecSetVariableStmt

## Location
[src/backend/utils/misc/guc_funcs.c:43-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L43-L166)

## Overview
Executes PostgreSQL SET statements, handling various types of variable assignments including regular GUC parameters, transaction-level settings, and special multi-value settings like TRANSACTION and SESSION CHARACTERISTICS.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function is the main executor for SET command statements in PostgreSQL. It handles different kinds of variable setting operations based on the VariableSetStmt's kind field:

- **VAR_SET_VALUE/VAR_SET_CURRENT**: Sets regular GUC parameters to specific values
- **VAR_SET_MULTI**: Handles complex multi-parameter settings like 'SET TRANSACTION' and 'SET SESSION CHARACTERISTICS'
- **VAR_SET_DEFAULT/VAR_RESET**: Resets parameters to their default values
- **VAR_RESET_ALL**: Resets all parameters to defaults

The function includes safety checks for parallel operations and proper transaction block warnings for local settings. It also invokes post-alter hooks for auditing purposes.

## Parameters / Member Variables
- : Pointer to VariableSetStmt structure containing the SET command details
- : Boolean indicating whether this is a top-level command (affects transaction warnings)

## Dependencies
- Functions called/Symbols referenced:
  - [set_config_option](../s/set_config_option.md)
  - [ExtractSetVariableArgs](ExtractSetVariableArgs.md)
  - [SetPGVariable](../S/SetPGVariable.md)
  - [WarnNoTransactionBlock](../W/WarnNoTransactionBlock.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - [ResetAllOptions](../R/ResetAllOptions.md)
  - [ImportSnapshot](../I/ImportSnapshot.md)
  - InvokeObjectPostAlterHookArgStr
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (in src/backend/tcop/utility.c:872)

## Notes and Other Information
- Blocks SET operations during parallel mode execution for worker synchronization safety
- Handles special SQL syntax cases like TRANSACTION SNAPSHOT that don't correspond to regular GUC variables  
- Distinguishes between session-level and local (transaction-level) variable settings
- Includes comprehensive error handling for unexpected SET command variants
- Supports both superuser and regular user permission levels through PGC_SUSET/PGC_USERSET contexts