# VariableSetStmt

## Location
src/include/nodes/parsenodes.h: 2618 - 2625

## Overview
VariableSetStmt is a parse tree node structure that represents SQL SET and RESET statements for modifying PostgreSQL configuration parameters and session variables.

## Definition
```c
typedef struct VariableSetStmt
{
    NodeTag         type;
    VariableSetKind kind;      /* type of SET operation */
    char           *name;      /* variable to be set */
    List           *args;      /* List of A_Const nodes */
    bool            is_local;  /* SET LOCAL? */
} VariableSetStmt;
```

Where VariableSetKind is defined as:
```c
typedef enum VariableSetKind
{
    VAR_SET_VALUE,    /* SET var = value */
    VAR_SET_DEFAULT,  /* SET var TO DEFAULT */
    VAR_SET_CURRENT,  /* SET var FROM CURRENT */
    VAR_SET_MULTI,    /* special case for SET TRANSACTION ... */
    VAR_RESET,        /* RESET var */
    VAR_RESET_ALL,    /* RESET ALL */
} VariableSetKind;
```

## Detailed Description
VariableSetStmt represents the parsed form of PostgreSQL SET and RESET statements, which are used to modify configuration parameters, session variables, and transaction characteristics. The structure handles various forms of variable assignment including setting specific values, resetting to defaults, and copying from current session values.

The statement supports both session-level and transaction-level variable modifications through the is_local flag. When is_local is true, the variable change applies only to the current transaction and is automatically reverted when the transaction ends.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a VariableSetStmt node in the parse tree
- `kind`: VariableSetKind enum specifying the type of SET operation (value assignment, default reset, etc.)
- `name`: String name of the configuration parameter or variable to modify
- `args`: List of A_Const nodes containing the values to assign (empty for RESET operations)
- `is_local`: Boolean flag indicating whether this is a SET LOCAL operation (transaction-scoped)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree node identification)
  - VariableSetKind (enumeration for SET operation types)
  - List (PostgreSQL list data structure)
  - A_Const (constant value nodes)

- Called from (representative examples):
  - AlterSetting
  - update_proconfig_value
  - PlannedStmtRequiresSnapshot
  - standard_ProcessUtility
  - CreateCommandTag
  - ExecSetVariableStmt
  - ExtractSetVariableArgs

## Notes and Other Information
- The structure handles both SET and RESET commands despite their semantic differences
- SET LOCAL changes are automatically reverted at transaction end, while regular SET changes persist for the session
- Special handling exists for SET TRANSACTION statements via VAR_SET_MULTI kind
- The distinction between VAR_SET_DEFAULT and VAR_RESET is preserved for command tagging purposes even though they are semantically equivalent
- Configuration parameters can include GUC (Grand Unified Configuration) settings, session variables, and transaction characteristics
- The args list typically contains a single A_Const for simple assignments but can contain multiple values for complex settings