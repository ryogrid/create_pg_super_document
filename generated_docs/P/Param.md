# Param

## Location
[src/include/nodes/primnodes.h:373-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L373-L385)

## Overview
The Param structure represents a parameter placeholder in PostgreSQL's expression tree, used for prepared statements, parameterized queries, and subquery parameter passing.

## Definition
```c
typedef struct Param
{
    Expr        xpr;
    ParamKind   paramkind;      /* kind of parameter. See above */
    int         paramid;        /* numeric ID for parameter */
    Oid         paramtype;      /* pg_type OID of parameter's datatype */
    int32       paramtypmod pg_node_attr(query_jumble_ignore);
    Oid         paramcollid pg_node_attr(query_jumble_ignore);
    ParseLoc    location;       /* token location, or -1 if unknown */
} Param;
```

## Detailed Description
The Param structure represents parameter placeholders within PostgreSQL's expression system. These parameters are essential for prepared statements, parameterized queries, and for passing values between different levels of nested queries. The structure supports different kinds of parameters (external parameters from prepared statements, PARAM_EXEC for subquery correlation, etc.) and maintains full type information for proper type checking and execution. Parameters enable PostgreSQL to separate query structure from data values, supporting both prepared statement execution and complex subquery operations.

## Parameters / Member Variables
- `xpr`: Base expression node structure containing common expression properties
- `paramkind`: Enumeration indicating the type of parameter (PARAM_EXTERN for external parameters, PARAM_EXEC for execution-time parameters, etc.)
- `paramid`: Numeric identifier for the parameter, unique within its parameter kind context
- `paramtype`: PostgreSQL type system OID identifying the parameter's expected data type
- `paramtypmod`: Type modifier value providing additional type-specific information, ignored during query jumbling
- `paramcollid`: OID of the collation for this parameter, or InvalidOid if no collation applies
- `location`: Token location in the original query text for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - [ParamKind](ParamKind.md)
  - ParseLoc
- Called from (representative examples):
  - [find_expr_references_walker](../f/find_expr_references_walker.md) (dependency tracking)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [sql_fn_make_param](../s/sql_fn_make_param.md) (function parameter creation)
  - Various optimizer and planner functions for parameter handling
  - Parser functions for parameter processing

## Notes and Other Information
- Extensively used throughout the PostgreSQL system for parameter management
- Critical for prepared statement functionality and query plan reuse
- Supports multiple parameter kinds for different execution contexts
- Type modifier and collation information ignored during query jumbling for optimization
- Location tracking enables precise error reporting for parameter-related issues
- Central to PostgreSQL's parameter substitution and subquery correlation mechanisms