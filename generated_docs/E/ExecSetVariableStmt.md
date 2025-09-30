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

## Simplified Source

```c
void ExecSetVariableStmt(VariableSetStmt *stmt, bool isTopLevel) {
    GucAction action = stmt->is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET;

    // Prevent SET operations during parallel mode
    if (IsInParallelMode()) {
        ereport(ERROR, "cannot set parameters during a parallel operation");
    }

    switch (stmt->kind) {
        case VAR_SET_VALUE:
        case VAR_SET_CURRENT:
            // Warn about SET LOCAL outside transaction blocks
            if (stmt->is_local) {
                WarnNoTransactionBlock(isTopLevel, "SET LOCAL");
            }

            // Set the configuration option
            set_config_option(stmt->name,
                            ExtractSetVariableArgs(stmt),
                            (superuser() ? PGC_SUSET : PGC_USERSET),
                            PGC_S_SESSION,
                            action, true, 0, false);
            break;

        case VAR_SET_MULTI:
            // Handle special multi-value settings
            if (strcmp(stmt->name, "TRANSACTION") == 0) {
                ListCell *head;
                WarnNoTransactionBlock(isTopLevel, "SET TRANSACTION");

                foreach(head, stmt->args) {
                    DefElem *item = (DefElem *) lfirst(head);

                    if (strcmp(item->defname, "transaction_isolation") == 0) {
                        SetPGVariable("transaction_isolation", list_make1(item->arg), stmt->is_local);
                    } else if (strcmp(item->defname, "transaction_read_only") == 0) {
                        SetPGVariable("transaction_read_only", list_make1(item->arg), stmt->is_local);
                    } else if (strcmp(item->defname, "transaction_deferrable") == 0) {
                        SetPGVariable("transaction_deferrable", list_make1(item->arg), stmt->is_local);
                    } else {
                        elog(ERROR, "unexpected SET TRANSACTION element: %s", item->defname);
                    }
                }
            } else if (strcmp(stmt->name, "SESSION CHARACTERISTICS") == 0) {
                ListCell *head;

                foreach(head, stmt->args) {
                    DefElem *item = (DefElem *) lfirst(head);

                    if (strcmp(item->defname, "transaction_isolation") == 0) {
                        SetPGVariable("default_transaction_isolation", list_make1(item->arg), stmt->is_local);
                    } else if (strcmp(item->defname, "transaction_read_only") == 0) {
                        SetPGVariable("default_transaction_read_only", list_make1(item->arg), stmt->is_local);
                    } else if (strcmp(item->defname, "transaction_deferrable") == 0) {
                        SetPGVariable("default_transaction_deferrable", list_make1(item->arg), stmt->is_local);
                    } else {
                        elog(ERROR, "unexpected SET SESSION element: %s", item->defname);
                    }
                }
            } else if (strcmp(stmt->name, "TRANSACTION SNAPSHOT") == 0) {
                A_Const *con = linitial_node(A_Const, stmt->args);

                if (stmt->is_local) {
                    ereport(ERROR, "SET LOCAL TRANSACTION SNAPSHOT is not implemented");
                }

                WarnNoTransactionBlock(isTopLevel, "SET TRANSACTION");
                ImportSnapshot(strVal(&con->val));
            } else {
                elog(ERROR, "unexpected SET MULTI element: %s", stmt->name);
            }
            break;

        case VAR_SET_DEFAULT:
            if (stmt->is_local) {
                WarnNoTransactionBlock(isTopLevel, "SET LOCAL");
            }
            // Fall through

        case VAR_RESET:
            // Reset to default value
            set_config_option(stmt->name, NULL,
                            (superuser() ? PGC_SUSET : PGC_USERSET),
                            PGC_S_SESSION,
                            action, true, 0, false);
            break;

        case VAR_RESET_ALL:
            // Reset all options to defaults
            ResetAllOptions();
            break;
    }

    // Invoke post-alter hook for auditing
    InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, stmt->name,
                                   ACL_SET, stmt->kind, false);
}
```