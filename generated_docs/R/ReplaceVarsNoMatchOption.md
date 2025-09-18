# ReplaceVarsNoMatchOption

## Location
src/include/rewrite/rewriteManip.h: 41 - 96

## Overview
An enumeration that defines the behavior when a variable replacement operation cannot find a matching target entry.

## Definition
```c
typedef enum ReplaceVarsNoMatchOption
{
    REPLACEVARS_REPORT_ERROR,       /* throw error if no match */
    REPLACEVARS_CHANGE_VARNO,       /* change the Var's varno, nothing else */
    REPLACEVARS_SUBSTITUTE_NULL,    /* replace with a NULL Const */
} ReplaceVarsNoMatchOption;
```

## Detailed Description
The `ReplaceVarsNoMatchOption` enumeration controls the behavior of variable replacement functions when they encounter a Var node that cannot be matched to a corresponding entry in a target list or similar structure. This provides flexibility in handling edge cases during query rewriting operations, allowing different strategies depending on the context of the transformation.

Each option represents a different error handling or fallback strategy: strict error reporting for cases where missing variables indicate a serious problem, variable number adjustment for cases where the structure is known to be correct but needs updating, or null substitution for cases where missing variables can be safely treated as unknown values.

## Parameters / Member Variables
- `REPLACEVARS_REPORT_ERROR`: Causes the function to throw an error when no matching variable is found, used when missing variables indicate a programming error or corrupted query structure
- `REPLACEVARS_CHANGE_VARNO`: Updates only the varno field of the Var node without changing its content, used when the variable exists but references need updating
- `REPLACEVARS_SUBSTITUTE_NULL`: Replaces the unmatched variable with a NULL constant of the appropriate type, used when missing variables can be treated as unknown values

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a standalone enum)
- Called from (representative examples):
  - [ReplaceVarsFromTargetList](ReplaceVarsFromTargetList.md) (src/backend/rewrite/rewriteManip.c:1778)
  - [map_variable_attnos](../m/map_variable_attnos.md) (src/backend/rewrite/rewriteManip.c:1664)

## Notes and Other Information
This enumeration is primarily used in functions like `ReplaceVarsFromTargetList` where variables from one query context need to be mapped to variables in another context. The choice of option depends on whether the caller expects all variables to have matches (use REPORT_ERROR), needs to update variable references (use CHANGE_VARNO), or can handle missing data gracefully (use SUBSTITUTE_NULL). This design supports robust error handling in PostgreSQL's complex query transformation pipeline.