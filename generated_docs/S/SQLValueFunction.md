# SQLValueFunction

## Location
[src/include/nodes/primnodes.h:1553-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1553-L1565)

## Overview
SQLValueFunction represents parameterless SQL functions with special grammar productions, including datetime value functions and general value specifications like CURRENT_DATE, CURRENT_USER, etc.

## Definition
```c
typedef enum SQLValueFunctionOp
{
    SVFOP_CURRENT_DATE,
    SVFOP_CURRENT_TIME,
    SVFOP_CURRENT_TIME_N,
    SVFOP_CURRENT_TIMESTAMP,
    SVFOP_CURRENT_TIMESTAMP_N,
    SVFOP_LOCALTIME,
    SVFOP_LOCALTIME_N,
    SVFOP_LOCALTIMESTAMP,
    SVFOP_LOCALTIMESTAMP_N,
    SVFOP_CURRENT_ROLE,
    SVFOP_CURRENT_USER,
    SVFOP_USER,
    SVFOP_SESSION_USER,
    SVFOP_CURRENT_CATALOG,
    SVFOP_CURRENT_SCHEMA,
} SQLValueFunctionOp;

typedef struct SQLValueFunction
{
    Expr        xpr;
    SQLValueFunctionOp op;      /* which function this is */

    /*
     * Result type/typmod.  Type is fully determined by "op", so no need to
     * include this Oid in the query jumbling.
     */
    Oid         type pg_node_attr(query_jumble_ignore);
    int32       typmod;
    ParseLoc    location;       /* token location, or -1 if unknown */
} SQLValueFunction;
```

## Detailed Description
SQLValueFunction is a node structure that represents special SQL functions that take no parameters and have dedicated grammar productions. These include datetime functions (like CURRENT_DATE, CURRENT_TIMESTAMP) and session information functions (like CURRENT_USER, CURRENT_SCHEMA). The SQL standard categorizes some of these as datetime value functions and others as general value specifications.

The structure stores the result type and typmod to avoid requiring each function to be handled individually in various parts of the code. All these functions return non-collating datatypes and are classified as stable functions, meaning they return the same result within a single SQL statement execution but may return different results across different statement executions.

## Parameters / Member Variables
- `xpr`: Base expression node containing common expression information
- `op`: SQLValueFunctionOp enum value specifying which specific function this represents (e.g., CURRENT_DATE, CURRENT_USER)
- `type`: OID of the result data type (marked as query_jumble_ignore since it's fully determined by the op field)
- `typmod`: Type modifier for the result, particularly important for datetime functions with precision
- `location`: Parse location in the original SQL text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - SQLValueFunctionOp (enum)
  - ParseLoc
  - Expr (base type)
  - Oid

- Called from (representative examples):
  - transformSQLValueFunction (parse_expr.c:2302)
  - ExecInitExprRec (execExpr.c:2249)
  - ExecEvalSQLValueFunction (execExprInterp.c:2642)
  - exprType (nodeFuncs.c:216)
  - get_rule_expr (ruleutils.c:9708)

## Notes and Other Information
- SQLValueFunction is part of PostgreSQL's expression node hierarchy, inheriting from the base Expr type
- The pg_node_attr(query_jumble_ignore) annotation on the type field helps optimize query plan caching since the type is fully determined by the operation
- These functions are classified as stable, meaning they return consistent results within a single statement but may vary between statements
- All supported functions return non-collating datatypes, eliminating the need for collation handling
- The _N variants (like CURRENT_TIME_N) represent functions that accept a precision parameter for fractional seconds
- Common examples include CURRENT_DATE (returns current date), CURRENT_USER (returns current user name), and CURRENT_SCHEMA (returns current schema name)
- The transformation from SQL syntax to this internal representation happens in transformSQLValueFunction
- Execution is handled by ExecEvalSQLValueFunction which evaluates the specific function based on the op field