# GucAction

## Location
src/include/utils/guc.h: 202 - 203

## Overview
GucAction is an enumeration type that specifies the action to be taken when setting a PostgreSQL configuration parameter, controlling the scope and duration of the parameter change.

## Definition
```c
typedef enum
{
    /* Types of set_config_option actions */
    GUC_ACTION_SET,     /* regular SET command */
    GUC_ACTION_LOCAL,   /* SET LOCAL command */
    GUC_ACTION_SAVE,    /* function SET option, or temp assignment */
} GucAction;
```

## Detailed Description
GucAction defines three distinct types of configuration parameter setting actions in PostgreSQL. Each action determines the scope and lifetime of a configuration change:

- **GUC_ACTION_SET**: Used for regular SET commands that affect the current session globally. Changes persist for the entire session or until explicitly changed again.

- **GUC_ACTION_LOCAL**: Used for SET LOCAL commands that make changes local to the current transaction. The parameter reverts to its previous value when the transaction ends (either commit or rollback).

- **GUC_ACTION_SAVE**: Used for function-level parameter settings and temporary assignments. This typically applies when a function needs to temporarily change a parameter for its execution duration, with automatic restoration afterward.

This enumeration is primarily used by the GUC (Grand Unified Configuration) system to manage PostgreSQL configuration parameters with different scoping semantics.

## Parameters / Member Variables
- `GUC_ACTION_SET`: Regular SET command - session-wide parameter change
- `GUC_ACTION_LOCAL`: SET LOCAL command - transaction-local parameter change  
- `GUC_ACTION_SAVE`: Function SET option or temporary assignment - limited scope parameter change

## Dependencies
- Functions that use GucAction:
  - set_config_option (src/backend/utils/misc/guc.c:3347)
  - set_config_option_ext (src/backend/utils/misc/guc.c:3387)
  - set_config_with_handle (src/backend/utils/misc/guc.c:3411)
  - push_old_value (src/backend/utils/misc/guc.c:2136)
  - ProcessGUCArray (src/backend/utils/misc/guc.c:6465)
  - ExecSetVariableStmt (src/backend/utils/misc/guc_funcs.c:45)
  - fmgr_security_definer (src/backend/utils/fmgr/fmgr.c:719)

- Used in structures:
  - GUCHashEntry (src/backend/utils/misc/guc.c:244)

## Notes and Other Information
- This enumeration is defined in src/include/utils/guc.h and is fundamental to PostgreSQL's configuration management system
- The action type affects how parameter changes are handled during transaction boundaries and function calls
- GUC_ACTION_LOCAL is particularly important for maintaining transaction isolation of parameter changes
- GUC_ACTION_SAVE is commonly used internally by PostgreSQL when functions need to temporarily modify parameters with automatic cleanup