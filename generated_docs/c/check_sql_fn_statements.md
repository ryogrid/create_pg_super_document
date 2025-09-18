# check_sql_fn_statements

## Location
src/backend/executor/functions.c: 1534 - 1608

## Overview
Validates SQL function statements to ensure they conform to PostgreSQL's restrictions and do not contain unsupported constructs.

## Definition
```c
void check_sql_fn_statements(List *queryTreeLists)
```

## Detailed Description
check_sql_fn_statements is a validation function that examines the parsed query trees of an SQL function to enforce PostgreSQL's rules and restrictions. Currently, it implements a specific check to prevent calling procedures with output arguments within SQL functions, as the current implementation would discard output values (except for the last statement). This restriction preserves the opportunity for future enhancement to properly handle output parameter assignment by name according to SQL standards.

The function iterates through a list of sublists containing Query nodes, examining each query for problematic constructs. It's designed to be extensible for additional validation rules as needed.

## Parameters / Member Variables
- `queryTreeLists`: A list of sublists, where each sublist contains Query nodes representing the parsed statements of an SQL function

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (macro for safe list traversal)
  - IsA (macro for type checking)
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md) (during function validation)
  - [init_sql_fcache](../i/init_sql_fcache.md) (during function cache initialization)

## Notes and Other Information
- This function is part of PostgreSQL's SQL function validation infrastructure
- Currently focuses on procedure call restrictions but is designed to be extensible for additional checks
- The restriction on procedures with output arguments is a conscious design choice to preserve future enhancement opportunities
- Throws ERROR-level exceptions when unsupported constructs are detected
- Used both during function creation/validation and runtime initialization
- Part of the broader SQL function execution framework in functions.c