# ParamRef

## Location
[src/include/nodes/parsenodes.h:301-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L301-L306)

## Overview
ParamRef is a parse tree node that represents parameter references ($n) in SQL statements, enabling parameterized queries and prepared statements.

## Definition
```c
typedef struct ParamRef
{
    NodeTag     type;
    int         number;         /* the number of the parameter */
    ParseLoc    location;       /* token location, or -1 if unknown */
} ParamRef;
```

## Detailed Description
ParamRef represents parameter placeholders in SQL queries, typically written as $1, $2, $3, etc. These parameter references are fundamental to PostgreSQL's prepared statement system, allowing queries to be parameterized with values supplied at execution time rather than being embedded directly in the SQL text.

When the parser encounters a parameter reference like "$1", it creates a ParamRef node with the parameter number. During query analysis and execution, these parameter references are resolved to their actual values through the parameter handling system.

This mechanism provides both performance benefits (through query plan reuse) and security benefits (by preventing SQL injection attacks when parameters are used properly).

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ParamRef node
- `number`: The parameter number (1-based, e.g., $1 has number=1)
- `location`: Source location of the parameter reference in the original SQL text

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - [transformParamRef](../t/transformParamRef.md)
  - [sql_fn_param_ref](../s/sql_fn_param_ref.md)
  - [paramlist_param_ref](../p/paramlist_param_ref.md)
  - [fixed_paramref_hook](../f/fixed_paramref_hook.md)
  - [variable_paramref_hook](../v/variable_paramref_hook.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)

## Notes and Other Information
- Parameter numbers are 1-based following SQL standard conventions
- Used extensively in prepared statements and parameterized queries
- Essential for PostgreSQL's parameter handling and prepared statement infrastructure
- Transformed during semantic analysis to resolve parameter types and values
- Supports both fixed and variable parameter reference handling modes
- Location information enables precise error reporting for parameter-related issues
- The parameter numbering must be sequential and start from 1 for proper query preparation
- Used in both user queries and internally generated SQL in stored functions